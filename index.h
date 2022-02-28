/*
    Index design on disk image:
    
    Index file synopsis page (i.e., File header page: page 0 of index file.):
        Index column name
        Index column type
        Index column length
        Number of slots per page
        Next empty page number
        Root page number

    Index page (node): 
        A page is a node in B+ tree.

        Logical structure of a B+ tree node:
            Left pointer 0 | index col 0 | left pointer 1 | index col 1 | ....| left pointer n-1 | index col n-1 | right pointer n

        -------------------------
        Index page header:
            Flag: Leaf page, Internal page.
            Right most page no.
            Number of used indice on current page
        -------------------------
        Index page contents: (Index slots)
            Index column | Page no.     (Left pointer)            | Slot no. (Left pointer)
                           (Table file page no. for leaf page;
                            Index file page no. for node page.)
*/

//TODO: Index slot deletion

#ifndef __INDEX_H__
#define __INDEX_H__

#include "db.h"
#include "page_cache.h"

enum index_column_type {LONG_LONG = 0x81, DOUBLE, FIXED_LENGTH_STRING};
enum index_page_flag {Leaf = 1, Internal};

/*Individual index node page header*/
struct index_node_header{
    enum index_page_flag flag;
    i64 rightmost_page_no;
    i64 curr_key_num;
};

/*Index page layout*/
struct index_node_page{
    struct index_node_header index_node_header;
    i64 index_slots[0];
};

/*Index slot*/
struct index_page_slot{
    void *index_column; //A pointer to variable of type 'long long', 'double', or 'fixed length string'.
    i64 page_no;
    i64 slot_no;
};

/*Index file header layout*/
struct index_file_header {
    char index_column_name[MAX_STRING_LENGTH + 1];
    enum index_column_type index_column_type;
    i64 index_column_length;
    i64 slot_num_per_page;
    i64 next_empty_page_no;
    i64 root_page_no;
};

//Incorporate meta information of an index file.
class index{
protected:
    union{
        char *page;
        struct index_file_header *index_file_header;
    };
    class page_cache *page_cache;
    class paged_file index_paged_file;

    //Internal function to create a new or open an existed index file.
    i64 open_paged_index_file(char *index_column_name, char *table_name);

private:
    //Fill the contents in 'index_slot' in the designated position 'pos' on a cached page.
    /*Preequisite: The page should be cached first.*/
    void fill_index_page_slot(char *pos, struct index_page_slot *index_slot);

    //Find a specific index key contained in 'index_slot' on a page.
    bool find_index_slot_on_page(class index_page *cursor, struct index_page_slot *index_slot);

    //Go through the tree until we reach the leaf node
    i64 scurry_to_leaf(class index_page *&cursor, struct index_page_slot *index_slot);

    //Find position to insert new key.
    i64 find_position_for_new_slot(class index_page *cursor, struct index_page_slot *index_slot, char *&insert_pos);

    //Find if the specific index page is full.
    inline bool is_index_page_full(class index_page *cursor);

    //Insert directly the new index slot if the page has at least one available free slot.
    i64 insert_index_slot_on_page(class index_page *cursor, struct index_page_slot *index_slot, char *insert_pos);

    //Fill split leaf nodes: cursor(old page to be split) & new_page.
    i64 fill_split_index_leaf_page(class index_page *cursor, class index_page *new_page, struct index_page_slot *index_slot, \
                              char *insert_pos);

    //Fill split internal nodes: parent(old parent to be split) & new_parent.
    i64 fill_split_index_internal_page(class index_page *parent, class index_page *new_parent, class index_page *new_child,
                                 struct index_page_slot *index_slot, char *insert_pos);

    //Fill new root page
    i64 fill_new_root_page(class index_page *root, class index_page *child_left, class index_page *child_right);

    //Find parent page
    i64 find_parent_index_page(class index_page *child, class index_page *&parent);

    //Insert to parent page
    i64 insert_to_parent_page(class index_page *parent, class index_page *new_sibling, class index_page *old_sibling);

public:
    index(class page_cache *page_cache) : page_cache(page_cache), page(nullptr){}

    //Create index file.
    i64 create_index(enum index_column_type type, char *table_name, char *index_column_name, i64 index_column_length);

    //Open existed index file.
    i64 open_index(char *table_name, char *index_column_name);

    //Close opened index file.
    i64 close_index();

    //Get maximum slot number on a page.
    inline i64 get_slot_num_per_page(){return index_file_header->slot_num_per_page;}

    //Search a specific index key in current index file.
    i64 search_key(struct index_page_slot *index_slot);

    //Insert an index slot into current index file.
    i64 insert(struct index_page_slot *index_slot);
};

//An individual index page (Internal or leaf page)
class index_page{
friend class index;
protected:    
    union{
        char *page;
        struct index_node_page *index_node_page;
    };
    i64 page_no;
    class paged_file *index_paged_file;

public:
    index_page(class paged_file *index_paged_file) : index_paged_file(index_paged_file) {}
    ~index_page();
    i64 create_empty_node(enum index_page_flag flag, i64 page_no);
};

extern void index_test();
extern void index_test2();

#endif