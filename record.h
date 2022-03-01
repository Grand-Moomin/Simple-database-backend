#ifndef __RECORD_H__
#define __RECORD_H__

/*
    Record design:

    Record file header: 
    Page 0:
        Header total pages.
        Table name: MAX_STRING_LENGTH
        Total columns(n).

    Page 1 ~ m: 
        Column 0 name: MAX_STRING_LENGTH
        Column 0 type: LONG_LONG, DOUBLE, or FIXED_LENGTH_STRING
        Column 0 length: sizeof(long long), sizeof(double) or string length
        Column 1 name: ...
        ...
        Column n-1 name: MAX_STRING_LENGTH
        Column n-1 type: LONG_LONG, DOUBLE, or FIXED_LENGTH_STRING
        Column n-1 length: sizeof(long long), sizeof(double) or string length

    Record page:

        A page includes at least one slot to store a record. 
        Since it is possible that a single record may need multiple pages, extended page is defined.
        Supported data types are LONG LONG, DOUBLE FLOAT, FIXED LENGTH STRING (max length: 256).

        ------------------------
        Page header:
            Page type   : Normal page, Extended page. 
                (Normal page: A page can contain at least one record. 
                 Extended page: A record that occupies multiple pages.)
            Next extended page  : -1 (No more extended pages), page no.
            Next record page    : -1 (No more pages), page no.
            Slot bitmap :
        ------------------------
        Page Contents:(Record slots)
            Record No. (Auto-increment, used for default primary index)
            Record1 : Data1, Data2, ...
            ...
            RecordN : Data1, Data2, ...

        
        Database meta information table:
            -------------------------------------------------------------------------------------------------
            Record no. | relation_name | relation_type | root_page no. | First available page | SQL to create the relation 
            -------------------------------------------------------------------------------------------------
*/

#include "index.h"

#define ceiling(nominator, denominator) ((nominator / denominator) + (nominator % denominator) ? 1 : 0)

struct column_meta{
    char name[MAX_STRING_LENGTH + 1];
    enum index_column_type type;
    i64 length;
};

struct record_file_header{
    i64 header_total_pages;
    char table_name[MAX_STRING_LENGTH + 1];
    i64 total_column_number;
    i64 next_available_page_no;
    i64 next_empty_page_no;
};

enum record_page_type {Normal = 0x40, Extended};

struct record_page_header{
    enum record_page_type record_page_type;
    i64 next_extended_page_no;
    i64 next_record_page_no;
    i64 slot_bitmap_length; //Unit: 8 bytes.
    i64 slot_bitmap[0];
};

struct record_slot_attribute{
    void *content;
    enum index_column_type type;
    i64 length;
};

class record{
private:
    struct record_file_header file_header;
    struct column_meta *column_meta_copy;

    i64 record_length;      //Each record length
    i64 records_per_page;   //The number of records a single page can contain. 0 for extended page.

    class page_cache *page_cache;
    class paged_file record_paged_file;

    inline i64 get_next_available_page_no() {return file_header.next_available_page_no;}
    inline i64 get_next_empty_page_no() {return file_header.next_empty_page_no;}
    inline void alter_next_empty_page_no() {file_header.next_empty_page_no++;} //TODO: Support deletion and extended page.
    i64 create_empty_record_page(i64 page_no);
    i64 find_first_empty_slot(i64 page_no, i64 &slot_no);

public:
    record(struct page_cache *page_cache) : page_cache(page_cache), column_meta_copy(nullptr) {}
    i64 create_record(char *file_name, struct column_meta *column_meta, i64 num_of_columns);
    i64 open_record(char *file_name);
    i64 close_record();
    i64 insert_record(struct record_slot_attribute *record, i64 column_num_of_record, i64 &page_no, i64 &slot_no);
    i64 get_record(struct record_slot_attribute *record, i64 column_num_of_record, i64 page_no, i64 slot_no);
};

#endif