#include "index.h"

//     Class index_page methods implementation
index_page::~index_page()
{
    index_paged_file->unpin_page(page_no);
}

i64 index_page::create_empty_node(enum index_page_flag flag, i64 page_no)
{
    if(page_no <= 0)
        return DB_ERROR;
    i64 ret = index_paged_file->get_page(page_no, this->page);
    if(ret != DB_SUCCESS)
        return ret;
    
    index_node_page->index_node_header.flag = flag;
    index_node_page->index_node_header.curr_key_num = 0;
    index_node_page->index_node_header.rightmost_page_no = 0;
    this->page_no = page_no;
    index_paged_file->mark_page_dirty(page_no);

    return DB_SUCCESS;
}

/* -------------------------------------- */
//    Class index methods implementation
i64 index::open_paged_index_file(char *table_name, char *index_column_name)
{
    char index_file_name[(MAX_STRING_LENGTH + 1) * 2];

    strncpy(index_file_name, table_name, MAX_STRING_LENGTH);
    strncat(index_file_name, ":", MAX_STRING_LENGTH);
    strncat(index_file_name, index_column_name, MAX_STRING_LENGTH);
    i64 ret = index_paged_file.open_paged_file(index_file_name, page_cache);
    return ret;
}

i64 index::create_index(enum index_column_type index_column_type, char *table_name, char *index_column_name, i64 index_column_length)
{
    //Create index file
    i64 ret = open_paged_index_file(table_name, index_column_name);
    if(ret != DB_SUCCESS)
        return DB_ERROR;

    //Get page 0 of index file and fill it with predefined info.
    ret = index_paged_file.get_page(0, this->page);
    if(ret != DB_SUCCESS) return ret;
    strncpy(this->index_file_header->index_column_name, index_column_name, MAX_STRING_LENGTH);
    this->index_file_header->index_column_type = index_column_type;
    this->index_file_header->index_column_length = index_column_length;
    this->index_file_header->slot_num_per_page = (PAGE_SIZE - sizeof(struct index_node_header))/(index_column_length + 2 * sizeof(i64));
    this->index_file_header->next_empty_page_no = 2;
    this->index_file_header->root_page_no = 1;
    //Mark file header page dirty.
    index_paged_file.mark_page_dirty(0);

    //Create an empty root page.
    class index_page index_node(&index_paged_file);
    return index_node.create_empty_node(Leaf, this->index_file_header->root_page_no);
}

i64 index::open_index(char *table_name, char *index_column_name)
{
    //Open index file
    i64 ret = open_paged_index_file(table_name, index_column_name);
    if(ret != DB_SUCCESS)
        return ret;
    
    //Get page 0 of index file.
    index_paged_file.get_page(0, this->page);
    return DB_SUCCESS;
}

i64 index::close_index(){
    return index_paged_file.close_paged_file();
}

i64 index::insert(struct index_page_slot *index_slot)
{
    char *pos, *insert_pos;
    class index_page *cursor = new class index_page(&index_paged_file);     //Cursor to iterate the tree.
    cursor->page_no = index_file_header->root_page_no;
    i64 ret = index_paged_file.get_page(cursor->page_no, cursor->page);
    if(ret != DB_SUCCESS)
        return ret;

    //If root is empty, insert the very first slot on the root page.
    if(!cursor->index_node_page->index_node_header.curr_key_num){
        //Make pos point to the first slot on the root page.
        pos = (char *)(cursor->index_node_page->index_slots);
        //Fill the first slot.
        fill_index_page_slot(pos, index_slot);
        //Now the root has one element.
        cursor->index_node_page->index_node_header.curr_key_num = 1;
        //Mark the root page dirty as it has been modified.
        index_paged_file.mark_page_dirty(cursor->page_no);
        return DB_SUCCESS;
    }

    //If the root is not empty.
    else{
        //Scurry to leaf node.
        scurry_to_leaf(cursor, index_slot);
        //Find the position to insert new key in the leaf node.
        if((ret = find_position_for_new_slot(cursor, index_slot, insert_pos)) != DB_SUCCESS)
            return ret;
        
        //If there is still some vacancy in the leaf node, insert it.
        if(is_index_page_full(cursor) == false){
            insert_index_slot_on_page(cursor, index_slot, insert_pos);
        }
        //Unfortunately, there is no vacancy in the leaf node, we have to split the node.
        else{
            class index_page *new_leaf = new class index_page(&index_paged_file);
            new_leaf->create_empty_node(Leaf, index_file_header->next_empty_page_no);
            index_file_header->next_empty_page_no++;    //We simply increment page number as we ignore the delete operation for now.
            index_paged_file.mark_page_dirty(0);        //Mark page 0 dirty since we changed the 'next_empty_page_no'.
            ret = fill_split_index_leaf_page(cursor, new_leaf, index_slot, insert_pos);
            if(ret != DB_SUCCESS){
                exit(-1); //Ignore the exception handling for now.
            }

            //If cursor's parent points to the root node, create a brand new root.
            if(cursor->page_no == index_file_header->root_page_no){
                class index_page *new_root = new class index_page(&index_paged_file);
                new_root->create_empty_node(Internal, index_file_header->next_empty_page_no);
                index_file_header->next_empty_page_no++;    //We simply increment page number as we ignore the delete operation for now.
                ret = fill_new_root_page(new_root, cursor, new_leaf);
                if(ret != DB_SUCCESS){
                    exit(-1); //Ignore the exception handling for now.
                }
                delete new_root;
            }

            //Or else, insert the last key of split cursor to its parent. 
            else{
                class index_page *parent = new class index_page(&index_paged_file);
                find_parent_index_page(cursor, parent);

                ret = insert_to_parent_page(parent, new_leaf, cursor);

                delete parent;
            }

            delete new_leaf;
        }
    }
    delete cursor;
    return ret;
}

i64 index::search_key(struct index_page_slot *index_slot)
{
    i64 i, node_key_num;
    class index_page *cursor = new class index_page(&index_paged_file);
    i64 ret = index_paged_file.get_page(index_file_header->root_page_no, cursor->page);//Start from the root page.
    cursor->page_no = index_file_header->root_page_no;
    if(ret != DB_SUCCESS)
        return ret;
    
    index_slot->page_no = -1; //To signal the caller that the specified index key is not found.
    index_slot->slot_no = -1;
    //If root is empty, return.
    node_key_num = cursor->index_node_page->index_node_header.curr_key_num;
    if(!node_key_num)
        return DB_SUCCESS;

    //Scurry to leaf node.
    if((ret = scurry_to_leaf(cursor, index_slot)) != DB_SUCCESS){
        return ret;
    }

    //Here, we arrived at a leaf node.
    node_key_num = cursor->index_node_page->index_node_header.curr_key_num;

    if(find_index_slot_on_page(cursor, index_slot)){
        delete cursor;
        return DB_SUCCESS;
    }

    delete cursor;
    return DB_SUCCESS;
}

void index::fill_index_page_slot(char *pos, struct index_page_slot *index_slot)
{
    switch(index_file_header->index_column_type){
        case FIXED_LENGTH_STRING:
            strncpy(pos, (const char *)(index_slot->index_column), index_file_header->index_column_length);
        break;
        default:
            memcpy(pos, index_slot->index_column, index_file_header->index_column_length);
        break;
    }
    i64 *tmp_pos = (i64 *)(pos + index_file_header->index_column_length);
    *tmp_pos++ = index_slot->page_no;
    *tmp_pos = index_slot->slot_no;
}

i64 index::scurry_to_leaf(class index_page *&cursor, struct index_page_slot *index_slot)
{
    i64 i, node_key_num, child_page_no, ret;
    i64 *tmp_pos;
    char *key = (char *)(index_slot->index_column);
    char *pos = (char *)(cursor->index_node_page->index_slots);int j = 0;

    while(cursor->index_node_page->index_node_header.flag == Internal){//cout<<j++<<"layer:"<<cursor->page_no<<endl;
        pos = (char *)(cursor->index_node_page->index_slots);
        node_key_num = cursor->index_node_page->index_node_header.curr_key_num;
        for(i = 0; i < node_key_num; ++i){
            if(index_file_header->index_column_type == FIXED_LENGTH_STRING){
                if(strncmp(key, pos, index_file_header->index_column_length) <= 0)
                    break;
            }
            else{
                if(memcmp(key, pos, index_file_header->index_column_length) <= 0)
                    break;
            }
            pos += index_file_header->index_column_length + sizeof(i64) * 2;
        }

        tmp_pos = (i64 *)(pos + index_file_header->index_column_length);
        if(i < node_key_num)
            child_page_no = *tmp_pos;
        else
            child_page_no = cursor->index_node_page->index_node_header.rightmost_page_no;

        cursor->index_paged_file->unpin_page(cursor->page_no);
        cursor->page_no = child_page_no;
        if((ret = cursor->index_paged_file->get_page(child_page_no,cursor->page)) != DB_SUCCESS)
            return ret;
    }
    return DB_SUCCESS;
}

bool index::find_index_slot_on_page(class index_page *cursor, struct index_page_slot *index_slot)
{
    bool found = false;
    i64 node_key_num = cursor->index_node_page->index_node_header.curr_key_num;
    char *key = (char *)index_slot->index_column;
    char *pos = (char *)(cursor->index_node_page->index_slots);

    for(i64 i = 0; i < node_key_num; ++i){
        if(index_file_header->index_column_type == FIXED_LENGTH_STRING){
            if(!strncmp(key, pos, index_file_header->index_column_length)){
                found = true;
                break;
            }
        }
        else{
            if(!memcmp(key, pos, index_file_header->index_column_length)){
                found = true;
                break;
            }
        }
        pos += index_file_header->index_column_length + sizeof(i64) * 2;
    }
    if(found == true){
        pos += index_file_header->index_column_length;
        memcpy(&index_slot->page_no, pos, sizeof(i64) * 2);
    }
    return found;
}

i64 index::find_position_for_new_slot(class index_page *cursor, struct index_page_slot *index_slot, char *&insert_pos)
{
    int res;
    i64 i;
    i64 node_key_num = cursor->index_node_page->index_node_header.curr_key_num;
    char *key = (char *)index_slot->index_column;
    char *pos = (char *)(cursor->index_node_page->index_slots);

    for(i = 0; i < node_key_num; ++i){
        if(index_file_header->index_column_type == FIXED_LENGTH_STRING){
            res = strncmp(key, pos, index_file_header->index_column_length);
            if(res < 0)
                break;
            else if(!res)
                goto duplicated_key_error; //Duplicated key not permitted.
        }
        else{
            res = memcmp(key, pos, index_file_header->index_column_length);
            if(res < 0)
                break;
            else if(!res)
                goto duplicated_key_error; //Duplicated key not permitted.
        }
        pos += index_file_header->index_column_length + sizeof(i64) * 2;
    }
    insert_pos = pos;
    return DB_SUCCESS;

duplicated_key_error:
    insert_pos = nullptr;
    return DB_ERROR;
}

inline bool index::is_index_page_full(class index_page *cursor)
{
    return cursor->index_node_page->index_node_header.curr_key_num >= index_file_header->slot_num_per_page;
}

i64 index::insert_index_slot_on_page(class index_page *cursor, struct index_page_slot *index_slot, char *insert_pos)
{
    if(is_index_page_full(cursor) == true)
        return DB_ERROR;
    
    int i;
    i64 node_key_num = cursor->index_node_page->index_node_header.curr_key_num;
    i64 index_slot_len = index_file_header->index_column_length + sizeof(i64) * 2;
    i64 pos = (insert_pos - (char *)cursor->index_node_page->index_slots) / index_slot_len;
    char *slot_pos = (char *)cursor->index_node_page->index_slots + node_key_num * index_slot_len;
    //Right shift existed slots.
    for(i = node_key_num; i > pos; --i){
        memcpy(slot_pos, slot_pos - index_slot_len, index_slot_len);
        slot_pos -= index_slot_len;
    }
    //Insert new slot.
    fill_index_page_slot(insert_pos, index_slot);

    //Increment slot number of current page.
    cursor->index_node_page->index_node_header.curr_key_num++;
    index_paged_file.mark_page_dirty(cursor->page_no);

    return DB_SUCCESS;
}

i64 index::fill_split_index_leaf_page(class index_page *cursor, class index_page *new_page, 
                                 struct index_page_slot *index_slot, char *insert_pos)
{
    if(!cursor || !new_page || !index_slot || !insert_pos)
        return DB_ERROR;

    int i;
    i64 index_slot_len = index_file_header->index_column_length + sizeof(i64) * 2;
    i64 pos = (insert_pos - (char *)cursor->index_node_page->index_slots) / index_slot_len;
    char *slot_pos = (char *)cursor->index_node_page->index_slots;
    i64 total_key_num = index_file_header->slot_num_per_page + 1;

    //Page cursor must be full, or the page should not be split.
    if(cursor->index_node_page->index_node_header.curr_key_num < index_file_header->slot_num_per_page)
        return DB_ERROR;

    //Create a temporary buffer to store all page index slot including the new one.
    char *buf = new char [index_slot_len * total_key_num];
    if(buf == nullptr)
        return DB_ERROR;

    char *buf_pos = buf;
    //Fill the temporary buffer.
    for(i = 0; i < total_key_num; ++i){
        if(i < pos){
            memcpy(buf_pos, slot_pos, index_slot_len);
            slot_pos += index_slot_len;
        }
        else if(i == pos){
            fill_index_page_slot(buf_pos, index_slot);
        }
        else{
            memcpy(buf_pos, slot_pos, index_slot_len);
            slot_pos += index_slot_len;
        }
        buf_pos += index_slot_len;
    }

    i64 cursor_key_num = cursor->index_node_page->index_node_header.curr_key_num = total_key_num / 2;
    i64 new_page_key_num = new_page->index_node_page->index_node_header.curr_key_num = total_key_num - cursor_key_num;
    new_page->index_node_page->index_node_header.flag = Leaf;

    buf_pos = buf;
    slot_pos = (char *)cursor->index_node_page->index_slots;
    memcpy(slot_pos, buf_pos, index_slot_len * cursor_key_num);

    buf_pos +=  index_slot_len * cursor_key_num;
    slot_pos = (char *)new_page->index_node_page->index_slots;
    memcpy(slot_pos, buf_pos, index_slot_len * new_page_key_num);

    //Mark modified pages dirty.
    index_paged_file.mark_page_dirty(cursor->page_no);
    index_paged_file.mark_page_dirty(new_page->page_no);

    delete [] buf;
    return DB_SUCCESS;
}

i64 index::fill_split_index_internal_page(class index_page *parent, class index_page *new_parent, class index_page *new_child,
                                 struct index_page_slot *index_slot, char *insert_pos)
{
    if(!parent || !new_parent || !index_slot || !insert_pos)
        return DB_ERROR;

    int i;
    i64 index_slot_len = index_file_header->index_column_length + sizeof(i64) * 2;
    i64 pos = (insert_pos - (char *)parent->index_node_page->index_slots) / index_slot_len;
    char *slot_pos = (char *)parent->index_node_page->index_slots;
    i64 total_key_num = index_file_header->slot_num_per_page + 1;

    //Page parent must be full, or the page should not be split.
    if(parent->index_node_page->index_node_header.curr_key_num < index_file_header->slot_num_per_page)
        return DB_ERROR;

    //Create a temporary buffer to store all page index slot including the new one.
    char *buf = new char [index_slot_len * (total_key_num)];
    if(buf == nullptr)
        return DB_ERROR;

    char *buf_pos = buf;
    //Fill the temporary buffer.
    for(i = 0; i < total_key_num; ++i){
        if(i < pos){
            memcpy(buf_pos, slot_pos, index_slot_len);
        }
        else if(i == pos){
            //Insert old sibling rightmost key and its corresponding page no.
            fill_index_page_slot(buf_pos, index_slot);
            buf_pos += index_slot_len;

            //All slots on parent page has been copied to the buffer, break.
            if(i == index_file_header->slot_num_per_page)
                break;

            //Assign new sibling page no to the key at original 'insert_pos'.
            memcpy(buf_pos, slot_pos, index_file_header->index_column_length);
            i64 *new_sibling_page_no = (i64*)((char *)buf_pos + index_file_header->index_column_length);
            *new_sibling_page_no = new_child->page_no;
        }
        else{
            //All slots on parent page has been copied to the buffer, break.
            if(i == index_file_header->slot_num_per_page) 
                break;
            memcpy(buf_pos, slot_pos, index_slot_len);
        }
        slot_pos += index_slot_len;
        buf_pos += index_slot_len;
    }

    i64 parent_key_num = parent->index_node_page->index_node_header.curr_key_num = total_key_num / 2;
    i64 new_parent_key_num = new_parent->index_node_page->index_node_header.curr_key_num = total_key_num - parent_key_num;
    new_parent->index_node_page->index_node_header.flag = Internal;

    buf_pos = buf;
    slot_pos = (char *)parent->index_node_page->index_slots;
    memcpy(slot_pos, buf_pos, index_slot_len * parent_key_num);

    buf_pos +=  index_slot_len * parent_key_num;
    slot_pos = (char *)new_parent->index_node_page->index_slots;
    memcpy(slot_pos, buf_pos, index_slot_len * new_parent_key_num);

    //Mark modified pages dirty.
    index_paged_file.mark_page_dirty(parent->page_no);
    index_paged_file.mark_page_dirty(new_parent->page_no);

    delete [] buf;
    return DB_SUCCESS;
}

i64 index::fill_new_root_page(class index_page *root, class index_page *left_child, class index_page *right_child)
{
    if(!root || !left_child || !right_child)
        return DB_ERROR;
    
    i64 index_slot_len = index_file_header->index_column_length + sizeof(i64) * 2;
    char *pos_root = (char *)root->index_node_page->index_slots;

    root->index_node_page->index_node_header.curr_key_num = 1;
    root->index_node_page->index_node_header.flag = Internal;

    char *left_child_max_key = (char *)left_child->index_node_page->index_slots + \
                               (left_child->index_node_page->index_node_header.curr_key_num - 1) * index_slot_len;
    memcpy(pos_root, left_child_max_key, index_file_header->index_column_length);
    pos_root += index_file_header->index_column_length;
    *(i64 *)pos_root = left_child->page_no; 
    root->index_node_page->index_node_header.rightmost_page_no = right_child->page_no;

    index_file_header->root_page_no = root->page_no;
    index_paged_file.mark_page_dirty(0);

    //Mark the new root page dirty.
    index_paged_file.mark_page_dirty(root->page_no);
    return DB_SUCCESS;
}

i64 index::find_parent_index_page(class index_page *child, class index_page *&parent)
{
    i64 i, node_key_num, child_page_no, ret;
    i64 *tmp_pos;
    class index_page *cursor = new class index_page(&index_paged_file);
    i64 cursor_parent_page_no = -1;
    char *key = (char *)(child->index_node_page->index_slots); //First key on child index page
    char *pos; 

    if(cursor == nullptr)
        return DB_ERROR;

    //Search from root page.
    cursor->page_no = index_file_header->root_page_no;
    index_paged_file.get_page(cursor->page_no, cursor->page);

    while(cursor->index_node_page->index_node_header.flag == Internal){
        //If the child is an internal page.
        if(child->page_no == cursor->page_no){
            parent->page_no = cursor_parent_page_no;
            index_paged_file.get_page(cursor_parent_page_no, parent->page);
            delete cursor;
            return DB_SUCCESS;
        }

        cursor_parent_page_no = cursor->page_no;
        pos = (char *)(cursor->index_node_page->index_slots);
        node_key_num = cursor->index_node_page->index_node_header.curr_key_num;
        for(i = 0; i < node_key_num; ++i){
            if(index_file_header->index_column_type == FIXED_LENGTH_STRING){
                if(strncmp(key, pos, index_file_header->index_column_length) <= 0)
                    break;
            }
            else{
                if(memcmp(key, pos, index_file_header->index_column_length) <= 0)
                    break;
            }
            pos += index_file_header->index_column_length + sizeof(i64) * 2;
        }

        tmp_pos = (i64 *)(pos + index_file_header->index_column_length);
        if(i < node_key_num)
            child_page_no = *tmp_pos;
        else
            child_page_no = cursor->index_node_page->index_node_header.rightmost_page_no;

        cursor->index_paged_file->unpin_page(cursor->page_no);
        cursor->page_no = child_page_no;
        if((ret = cursor->index_paged_file->get_page(child_page_no,cursor->page)) != DB_SUCCESS)
            return ret;
    }

    //If we reach here, the child must be a leaf.
    if(child->page_no == cursor->page_no){
        parent->page_no = cursor_parent_page_no;
        index_paged_file.get_page(cursor_parent_page_no, parent->page);
    }

    delete cursor;
    return DB_SUCCESS;
}

i64 index::insert_to_parent_page(class index_page *parent, class index_page *new_sibling, class index_page *old_sibling)
{
    //Find the position where the last key of old sibling should be inserted.
    i64 ret, insert_pos_i;
    char *insert_pos;
    char *pos = (char *)(old_sibling->index_node_page->index_slots);
    i64 index_slot_len = index_file_header->index_column_length + sizeof(i64) * 2;
    pos += (old_sibling->index_node_page->index_node_header.curr_key_num - 1) * index_slot_len;

    //We want to insert the old_siblings largest key to the parent.
    struct index_page_slot rightmost_slot;
    rightmost_slot.index_column = pos;
    rightmost_slot.page_no = old_sibling->page_no;
    rightmost_slot.slot_no = 0;

    //Find the position to insert new key in this parent node.
    if((ret = find_position_for_new_slot(parent, &rightmost_slot, insert_pos)) != DB_SUCCESS)
        return ret;

    //Get insert position offset starting from 'index_slots'.
    insert_pos_i = (insert_pos - (char *)parent->index_node_page->index_slots) / index_slot_len;
    //If there is still some vacancy in this parent node, insert it.
    if(is_index_page_full(parent) == false){
        //If we have to insert new slot to the rightmost position of parent, change the rightmost page number to the new sibling's.        
        if(insert_pos_i  == parent->index_node_page->index_node_header.curr_key_num){
            parent->index_node_page->index_node_header.rightmost_page_no = new_sibling->page_no;
            //cout<<parent->page_no<<' '<<parent->index_node_page->index_node_header.rightmost_page_no<<endl;
        }
        insert_index_slot_on_page(parent, &rightmost_slot, insert_pos);

        //Adjust new sibling page no. in parent node if it is not at the rightmost position of parent.
        if(insert_pos_i  < parent->index_node_page->index_node_header.curr_key_num){
            //Change the page no. of next slot of 'insert_pos' to that of 'new_sibling'.
            i64 *new_sibling_page_no = (i64 *)(insert_pos + index_slot_len + index_file_header->index_column_length);
            *new_sibling_page_no = new_sibling->page_no;
        }
    }

    //Unfortunately, there is no vacancy in the parent node, we have no choice but to split the parent as well.
    else{
        class index_page *new_parent = new class index_page(&index_paged_file); 
        new_parent->create_empty_node(Internal, index_file_header->next_empty_page_no); 
        index_file_header->next_empty_page_no++;    //We simply increment page number as we ignore the delete operation for now.

        //If we have to insert new slot to the rightmost position of parent, change the rightmost page number of the new parent to the new sibling's. 
        if(insert_pos_i == parent->index_node_page->index_node_header.curr_key_num){
            new_parent->index_node_page->index_node_header.rightmost_page_no = new_sibling->page_no;
        }
        //Or, assign the rightmost page number of the old parent to the new one's.
        else{
            new_parent->index_node_page->index_node_header.rightmost_page_no = parent->index_node_page->index_node_header.rightmost_page_no;
        }

        ret = fill_split_index_internal_page(parent, new_parent, new_sibling, &rightmost_slot, insert_pos);
        if(ret != DB_SUCCESS){
            exit(-1); //Ignore exception handling for now.
        }

        //Set parent rightmost page number to the one correspoinding to its current last key.
        pos = (char *)(parent->index_node_page->index_slots);
        pos += (parent->index_node_page->index_node_header.curr_key_num - 1) * index_slot_len + index_file_header->index_column_length;
        parent->index_node_page->index_node_header.rightmost_page_no = *(i64 *)pos;

        //If cursor's parent points to the root node, create a brand new root.
        if(parent->page_no == index_file_header->root_page_no){
            class index_page *new_root = new class index_page(&index_paged_file);
            new_root->create_empty_node(Internal, index_file_header->next_empty_page_no);
            index_file_header->next_empty_page_no++;    //We simply increment page number as we ignore the delete operation for now.
            ret = fill_new_root_page(new_root, parent, new_parent);
            if(ret != DB_SUCCESS){
                exit(-1); //Ignore exception handling for now.
            }
            delete new_root;
        }

        //Or else, insert the last key of split cursor to its parent. 
        else{
            class index_page *grand_parent = new class index_page(&index_paged_file);
            find_parent_index_page(parent, grand_parent);

            insert_to_parent_page(grand_parent, new_parent, parent);

            delete grand_parent;
        }

        //Last key of current parent is meaningless, so we ignore it by decreasing the key number by 1.
        parent->index_node_page->index_node_header.curr_key_num--;

        delete new_parent;
    }
    return ret;
}


// Test stub
//#define CREAT_INDEX_FILE
//#define INSERT_INDEX_SLOT

void index_test(){
    char idx_name[] = "FruitName";
    char tbl_name[] = "Fruit";
    class page_cache page_cache(20);
    class index idx(&page_cache);
#ifdef CREAT_INDEX_FILE
    idx.create_index(FIXED_LENGTH_STRING, tbl_name, idx_name, (MAX_STRING_LENGTH + 1));
    idx.close_index();
#endif
    
    idx.open_index(tbl_name, idx_name);
    char index_col_content[] = "whatever";
    char index_col_inst[sizeof(index_col_content) + 3] = {0}; 
    int index_col_len = strlen(index_col_content);
    strncpy(index_col_inst, index_col_content, index_col_len);
    char ordinal1 = 'c';
    char ordinal2 = '0';
    for(int i = 0; i < 0x41e0; ++i){
        if(!ordinal1){
            ordinal1++;
        }
        index_col_inst[index_col_len] = ordinal1;
        index_col_inst[index_col_len + 1] = ordinal2++;
        if(ordinal2 == '9' + 1){
            ordinal2 = 'A';
        }
        else if(ordinal2 == 'z' + 1){
            ordinal2 = '0';
            ordinal1++;
        }

        if(ordinal1 == '9' + 1){
            ordinal1 = 'A';
        }

        struct index_page_slot index_slot;
        index_slot.index_column = index_col_inst;
#ifdef INSERT_INDEX_SLOT
        index_slot.page_no = 0x55AA + i;
        index_slot.slot_no = i;
        idx.insert(&index_slot);
#endif
        idx.search_key(&index_slot);
        if(index_slot.slot_no!= i){
            cout<<"Err"<<i<<" ordinal1 "<<hex<<static_cast<int>(static_cast<unsigned char>(ordinal1))<<\
            " index: "<<(char *)index_slot.index_column<<':'<<index_slot.slot_no<<endl; pause();
        }
        cout<<(char *)index_slot.index_column<<' '<<index_slot.page_no<<' '<<index_slot.slot_no<<endl;
    }    
    idx.close_index();    
}

void index_test2()
{
    class page_cache page_cache(30);
    class index idx2(&page_cache);
    char idx_name2[] = "FruitNum";
    char tbl_name2[] = "Fruit";

#ifdef CREAT_INDEX_FILE
    idx2.create_index(LONG_LONG, tbl_name2, idx_name2, sizeof(long long));
    idx2.close_index();
#endif

    idx2.open_index(tbl_name2, idx_name2);
    long long index_col_inst2 = 0x0;
    struct index_page_slot index_slot2;

    for(int i = 0; i < 0x10000; ++i){
        index_slot2.index_column = &index_col_inst2;
#ifdef INSERT_INDEX_SLOT
        index_slot2.page_no = i / 10;
        index_slot2.slot_no = i;
        idx2.insert(&index_slot2);
#endif
        idx2.search_key(&index_slot2);
        if(index_slot2.slot_no!= i){
            cout<<"Err"<<i<<" index: "<<*(long long *)index_slot2.index_column<<':'<<index_slot2.slot_no<<endl; pause();
        }
        //cout<<*(long long *)index_slot2.index_column<<' '<<index_slot2.page_no<<' '<<index_slot2.slot_no<<endl;
        index_col_inst2++;
    }

    idx2.close_index();
}