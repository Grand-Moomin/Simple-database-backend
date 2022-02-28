#ifndef __PAGE_CACHE_H__
#define __PAGE_CACHE_H__

#include "db.h"

/*
    Page cache design:
        All pages: Dynamically allocated memory pool for cached pages.
        Pinned pages: -> hash table -> double linked list
        Free pages list: -> double linked list

    Page meta:
        Tuple (File descriptor, page no.) identifies an unique page.
        Dirty flag: if set, this page needs to be flush to disk when it is reused.
        Pinned flag: if set, this page is pinned, or else, it is free to be recycled. Used to prevent repeatedly releasing the same page.
        Adjacent pages in hash table.
        Adjacent pages in free pages list.
        Adjacent pages in the same file.
        The page contents.

    File session handle:
        File descriptor.
        Pointer to page cache.
        Cached pages of this file. (A list)
*/

struct page_meta {
    int fd;
    i64 page_no;
    int dirty;
    int pinned;
    struct double_linked_list_head adjacent_pages_in_hash_table; //Adjacent pages in hash bucket
    struct double_linked_list_head adjacent_pages_in_free_list;  //Adjacent pages in free page list
    struct double_linked_list_head adjacent_pages_in_file;       //Adjacent pages in the same file
    char page[PAGE_SIZE];
};

class page_cache{
private:
    static const int factor = 10;                //Bucket factor
    struct page_meta *all_pages;                 //All pages
    struct double_linked_list_head *page_bucket; //Pinned pages
    struct double_linked_list_head free_pages;   //Unpinned pages (Free pages list)
    int total_pages, bucket_size;

    int hash(int fd, i64 page_no);

public:
    page_cache(int bucket_size);
    ~page_cache();
    struct page_meta *get_page(int fd, i64 page_no, class paged_file *paged_file = nullptr);
    void remove_page_from_hash_table(struct page_meta *curr_page);
    void insert_page_to_free_list(struct page_meta *curr_page);     //Insert current page to the tail of free pages list.
};

class paged_file{
    int fd;
    class page_cache *page_cache;
    struct double_linked_list_head pages_in_file;
    i64 unpin_page_internal(struct page_meta *curr_page);

public:
    inline struct double_linked_list_head *get_pages_in_file() {return &pages_in_file;}
    i64 open_paged_file(char *filename, class page_cache *page_cache);
    i64 get_page(i64 page_no, char *&page);
    i64 unpin_page(i64 page_no);
    i64 mark_page_dirty(i64 page_no);
    i64 commit_page(i64 page_no);
    i64 close_paged_file();
};

extern void page_cache_test();

extern void page_cache_test2();

#endif