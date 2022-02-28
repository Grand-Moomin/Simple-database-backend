#include "page_cache.h"

i64 paged_file::unpin_page_internal(struct page_meta *curr_page)
{
    if(curr_page == nullptr)
        return DB_ERROR;
    //Insert this unpinned page to free page list.
    page_cache->insert_page_to_free_list(curr_page);

    return DB_SUCCESS;
}

i64 paged_file::unpin_page(i64 page_no)
{
    struct page_meta *page_info = page_cache->get_page(fd, page_no);
    return unpin_page_internal(page_info);
}

i64 paged_file::open_paged_file(char *filename, class page_cache *page_cache)
{
    this->page_cache = page_cache;
    init_double_linked_list_head(&this->pages_in_file);
    fd = open(filename, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if(fd == -1)
        return DB_ERROR;
    return DB_SUCCESS;
}

i64 paged_file::get_page(i64 page_no, char *&page)
{
    off_t ret = lseek(fd, page_no * PAGE_SIZE, SEEK_SET);
    if(ret == -1)
        return DB_ERROR;

    struct page_meta *page_info = page_cache->get_page(fd, page_no, this);
    if(page_info == nullptr){
        return DB_ERROR;
    }
    if(!page_info->pinned){
        page_info->pinned = 1;
        delete_double_linked_list_entry(&page_info->adjacent_pages_in_free_list);
        init_double_linked_list_head(&page_info->adjacent_pages_in_free_list);
    }
    page = page_info->page;

    return DB_SUCCESS;
}

i64 paged_file::mark_page_dirty(i64 page_no)
{
    struct page_meta *page_info = page_cache->get_page(fd, page_no);
    if(page_info == nullptr)
        return DB_ERROR;
    page_info->dirty = 1;
    return DB_SUCCESS;
}

i64 paged_file::commit_page(i64 page_no)
{
    struct page_meta *page_info = page_cache->get_page(fd, page_no);
    if(page_info == nullptr)
        return DB_ERROR;
    if(page_info->dirty){
        lseek(fd, page_info->page_no * PAGE_SIZE, SEEK_SET);
        write(fd, page_info->page, PAGE_SIZE);
        page_info->dirty = 0;
    }
    page_cache->insert_page_to_free_list(page_info);
    return DB_SUCCESS;
}

i64 paged_file::close_paged_file()
{
    struct page_meta *curr_page;
    double_linked_list_for_each_entry(curr_page, &pages_in_file, adjacent_pages_in_file){
        if(curr_page->dirty){
            lseek(fd, curr_page->page_no * PAGE_SIZE, SEEK_SET);
            write(fd, curr_page->page, PAGE_SIZE);
            curr_page->dirty = 0;
        }cout<<curr_page->page_no<<' ';
        //Remove current page from lists in hash buckets.
        page_cache->remove_page_from_hash_table(curr_page);
        //Release pages used by current file and add it to the tail of free pages list.
        page_cache->insert_page_to_free_list(curr_page);

        //If the direct previous element is not the list head 'pages_in_file', we reset its contents.
        //We do not reset the head for now, since there exists a corner case: 
        //      the previous element of the head can not be reset in this iteration as it is the last iterated element.
        struct double_linked_list_head *prev_elem = curr_page->adjacent_pages_in_file.prev;
        if(prev_elem != &pages_in_file) 
            init_double_linked_list_head(prev_elem);

    }cout<<"fd: "<<fd<<endl;

    //Corner case: Reset the previous element of the head.
    init_double_linked_list_head(pages_in_file.prev);
    
    //The following works the same way as the snippet above except we iterate the list directly without using the macro. 
    //A benifit of direct iteration is the elimination of the corner case.
    /*struct double_linked_list_head *cursor = pages_in_file.next;
    while(cursor != &pages_in_file){
        curr_page = container_of(cursor, struct page_meta, adjacent_pages_in_file);
        if(curr_page->dirty){
            lseek(fd, curr_page->page_no * PAGE_SIZE, SEEK_SET);
            write(fd, curr_page->page, PAGE_SIZE);
            curr_page->dirty = 0;
        }cout<<curr_page->page_no<<' ';
        //Remove current page from lists in hash buckets.
        page_cache->remove_page_from_hash_table(curr_page);
        //Release pages used by current file and add it to the tail of free pages list.
        page_cache->insert_page_to_free_list(curr_page);
        struct double_linked_list_head *cursor_to_be_reset = cursor;
        cursor = cursor->next;
        init_double_linked_list_head(cursor_to_be_reset);
    }cout<<"fd: "<<fd<<endl;*/

    //Now we can safely reset the list head per se.
    init_double_linked_list_head(&pages_in_file);

    close(fd);
    return DB_SUCCESS;
}

int page_cache::hash(int fd, i64 page_no)
{
    int res = fd;
    res = res * 147 + (int)page_no;
    res = res % bucket_size; //It is possible that the modulus is negative as C99 requires that (a/b) * b + a%b shall equal a. 
    return (res >= 0) ? res : res + bucket_size;
}

page_cache::page_cache(int total_pages)
{
    this->total_pages = total_pages;
    this->bucket_size = total_pages / factor;

    page_bucket = new struct double_linked_list_head [bucket_size];
    if(page_bucket == nullptr)
        exit(-1);
    memset(page_bucket, 0, sizeof(struct double_linked_list_head) * bucket_size);
    for(int i = 0; i < bucket_size; ++i){
        init_double_linked_list_head(&page_bucket[i]);
    }

    all_pages = new struct page_meta [total_pages];
    if(all_pages == nullptr){
        delete [] page_bucket;
        exit(-1);
    }
    memset(all_pages, 0, sizeof(struct page_meta) * total_pages);

    init_double_linked_list_head(&free_pages);
    for(int i = 0; i < total_pages; ++i){
        double_linked_list_add_head(&all_pages[i].adjacent_pages_in_free_list, &free_pages);
    }
    //The snippet below is more efficient than the previous one, although the previous one seems more succinct. 
    /*for(int i = 0; i < total_pages - 1; ++i){
        all_pages[i].adjacent_pages_in_free_list.next = &(all_pages[i + 1].adjacent_pages_in_free_list);
        all_pages[i + 1].adjacent_pages_in_free_list.prev = &(all_pages[i].adjacent_pages_in_free_list);
    }
    all_pages[0].adjacent_pages_in_free_list.prev = &free_pages;
    all_pages[total_pages - 1].adjacent_pages_in_free_list.next = &free_pages;
    free_pages.next = &(all_pages[0].adjacent_pages_in_free_list);
    free_pages.prev = &(all_pages[total_pages - 1].adjacent_pages_in_free_list);*/
}

page_cache::~page_cache()
{
    delete [] all_pages;
    delete [] page_bucket;
}

struct page_meta *page_cache::get_page(int fd, i64 page_no, class paged_file *paged_file)
{
    //Search hash bucket. If the required page is already pinned, return it directly.
    int hash_key = hash(fd, page_no);
    struct page_meta *curr_page;
    double_linked_list_for_each_entry(curr_page, &page_bucket[hash_key], adjacent_pages_in_hash_table){
        if(curr_page->fd == fd && curr_page->page_no == page_no){
            return curr_page;
        }
    }

    //No free pages in the free pages list because all pages are pinned.
    if(double_linked_list_empty(&free_pages)){
        cout<<"Page cache depleted. Can not insert page "<<page_no<<'.'<<endl;
        return nullptr;
    }

    //Get a free page from the head
    struct page_meta *new_page = container_of(free_pages.next, struct page_meta, adjacent_pages_in_free_list);
    //If the page is dirty, write it back to the disk.
    if(new_page->dirty){
        lseek(new_page->fd, new_page->page_no * PAGE_SIZE, SEEK_SET);
        write(new_page->fd, new_page->page, PAGE_SIZE); 
    }
    //Disconnect it from the hash table if necessary.
    if(new_page->adjacent_pages_in_hash_table.next && new_page->adjacent_pages_in_hash_table.prev){
        remove_page_from_hash_table(new_page);
    }
    //Disconnect it from cached file page list if necessary.
    if(new_page->adjacent_pages_in_file.next && new_page->adjacent_pages_in_file.prev){
        //cout<<"Delete file page://// old page no."<<new_page->page_no<<" old fd "<<new_page->fd<<endl;
        delete_double_linked_list_entry(&new_page->adjacent_pages_in_file);
        init_double_linked_list_head(&new_page->adjacent_pages_in_file);
    }

    //Adjust free pages list (REMOVE new page from the HEAD of free pages list) and fill in new page.
    delete_double_linked_list_entry(&new_page->adjacent_pages_in_free_list);
    init_double_linked_list_head(&new_page->adjacent_pages_in_free_list);

    new_page->dirty = 0;
    new_page->pinned = 1;
    new_page->fd = fd;
    new_page->page_no = page_no;
    //Pin the page in the HEAD of one hash bucket
    double_linked_list_add_head(&new_page->adjacent_pages_in_hash_table, &page_bucket[hash_key]);
    //Insert the new retrieved page to the HEAD of cached file page list.
    if(paged_file){
        //cout<<"Insert file page: new page no."<<new_page->page_no<<" new fd "<<new_page->fd<<endl;
        double_linked_list_add_head(&new_page->adjacent_pages_in_file, paged_file->get_pages_in_file());
    }

    memset(new_page->page, 0, PAGE_SIZE);
    lseek(fd, page_no * PAGE_SIZE, SEEK_SET);
    size_t nbytes = read(fd, new_page->page, PAGE_SIZE);

    //If file read fails, return the page back to page cache.
    if(nbytes == -1){
        new_page->pinned = 0;
        remove_page_from_hash_table(new_page);
        insert_page_to_free_list(new_page);
        return nullptr;
    }

    return new_page;
}

void page_cache::remove_page_from_hash_table(struct page_meta *curr_page)
{
    //Delete current node from the list.
    delete_double_linked_list_entry(&curr_page->adjacent_pages_in_hash_table);
    init_double_linked_list_head(&curr_page->adjacent_pages_in_hash_table);
}

void page_cache::insert_page_to_free_list(struct page_meta *curr_page)
{
    if(!curr_page->pinned)
        return;
    curr_page->pinned = 0;
    //Insert current page to the TAIL of free pages list.
    double_linked_list_add_tail(&(curr_page->adjacent_pages_in_free_list), &free_pages);
}

//Open different files simultaneously.
void page_cache_test()
{
    class page_cache pg_cache(40);
    class paged_file file, file2;
    char filename[] = "a.txt";
    char filename2[] = "b.txt";
    char *page;

    file.open_paged_file(filename, &pg_cache);
    file2.open_paged_file(filename2, &pg_cache);

    file.get_page(0, page);
    for(int i = 0; i < PAGE_SIZE/2; ++i){
        page[i] = 'e';
    }
    file.mark_page_dirty(0);

    file2.get_page(1, page);
    page[16] = 'w';
    file2.mark_page_dirty(1);

    file.get_page(1, page);
    page[1] = 'c';
    file.mark_page_dirty(1);

    file.get_page(3,page);
    page[8] = 'f';
    file.unpin_page(0);

    file.get_page(0,page);
    page[0] = 'h';
    file.unpin_page(1);
    file.unpin_page(3);
    //file.unpin_page(0);
    file.get_page(0,page);
    file.mark_page_dirty(3);

    for(int j= 5; j < 96; ++j){
        file.get_page(j,page);
        page[j+1] = 'a' + j % 26;
        file.mark_page_dirty(j);
        file.unpin_page(j);
    }

    file.close_paged_file();
    file2.close_paged_file();
}

//Open same file multiple times.
void page_cache_test2()
{
    class page_cache pg_cache(10);
    class paged_file file;
    char filename[] = "c.txt";
    char *page;

    file.open_paged_file(filename, &pg_cache);

    file.get_page(0, page);
    for(int i = 0; i < PAGE_SIZE/2; ++i){
        page[i] = 'e';
    }
    file.mark_page_dirty(0);

    file.get_page(1, page);
    page[16] = 'w';
    file.mark_page_dirty(1);

    file.get_page(1, page);
    page[1] = 'c';
    file.mark_page_dirty(1);

    file.get_page(3,page);
    page[8] = 'f';
    file.unpin_page(0);

    file.get_page(0,page);
    page[0] = 'h';
    file.unpin_page(1);
    file.unpin_page(3);
    //file.unpin_page(0);
    file.get_page(0,page);
    file.mark_page_dirty(3);

    file.close_paged_file();

    file.open_paged_file(filename, &pg_cache);

    for(int j= 5; j < 50; ++j){
        file.get_page(j,page);
        page[j+1] = 'a' + j % 26;
        file.mark_page_dirty(j);
        file.unpin_page(j);
    }    
    file.close_paged_file();
}