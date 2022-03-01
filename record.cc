#include "record.h"

i64 record::create_empty_record_page(i64 page_no)
{
    char *page = nullptr;
    i64 ret = record_paged_file.get_page(page_no, page);
    struct record_page_header *pg_hdr = (struct record_page_header *)page;

    if(ret != DB_SUCCESS)
        return ret;
    
    if(records_per_page > 0){
        pg_hdr->next_extended_page_no = -1;
        pg_hdr->next_record_page_no = -1;
        pg_hdr->record_page_type = Normal;
        pg_hdr->slot_bitmap_length = ceiling(records_per_page, sizeof(i64));
    }
    else{//TODO: support extended page.
        pg_hdr->next_extended_page_no = page_no + 1;
        pg_hdr->next_record_page_no = -1;
        pg_hdr->record_page_type = Extended;
        pg_hdr->slot_bitmap_length = 0;
    }

    record_paged_file.mark_page_dirty(page_no);
    record_paged_file.unpin_page(page_no);

    return DB_SUCCESS;
}

i64 record::find_first_empty_slot(i64 page_no, i64 &slot_no)
{
    char *page = nullptr;
    slot_no = -1;
    i64 ret = record_paged_file.get_page(page_no, page);
    if(ret != DB_SUCCESS)
        return ret;

    struct record_page_header *pg_hdr = (struct record_page_header *)page;
    i64 *bitmap = pg_hdr->slot_bitmap;
    i64 len = pg_hdr->slot_bitmap_length;
    i64 cur_slot_no = 0;

    for(int i = 0; i < len; ++i){
        i64 tmp = bitmap[i];
        for(int j = 0; j < sizeof(bitmap[0]) * 8; ++j){
            if(cur_slot_no >= records_per_page){
                record_paged_file.unpin_page(page_no);
                return DB_ERROR;
            }
            if(!(tmp & 1)){
                slot_no = cur_slot_no;
                bitmap[i] |= 1 << (cur_slot_no % (sizeof(bitmap[0]) * 8));
                record_paged_file.mark_page_dirty(page_no);
                record_paged_file.unpin_page(page_no);
                return DB_SUCCESS;
            }
            tmp >>= 1;
            cur_slot_no++;
        }
    }
    record_paged_file.unpin_page(page_no);
    return DB_ERROR;
}

i64 record::insert_record(struct record_slot_attribute *record, i64 column_num_of_record, i64 &page_no, i64 &slot_no)
{
    if(column_num_of_record != file_header.total_column_number)
        return DB_ERROR;

    page_no = get_next_available_page_no();
    slot_no = -1;
    find_first_empty_slot(page_no, slot_no);

    if(slot_no < 0){
        file_header.next_available_page_no = page_no = get_next_empty_page_no();
        find_first_empty_slot(page_no, slot_no);
    }

    if(slot_no < 0){
        cout<<"Fatal error: Database file corrupted."<<endl;
        return DB_ERROR;
    }

    if(page_no == get_next_empty_page_no()){
        alter_next_empty_page_no();
        create_empty_record_page(file_header.next_empty_page_no);
    }

    struct column_meta *cur_column_meta = column_meta_copy;
    char *page = nullptr;
    struct record_slot_attribute *cur_attr = record;
    record_paged_file.get_page(page_no, page);

    for(int i = 0; i < file_header.total_column_number; ++i){
        if(cur_column_meta->type != cur_attr->type || cur_column_meta->length != cur_attr->length){
            record_paged_file.unpin_page(page_no);
            return DB_ERROR;
        }
        cur_column_meta++;
        cur_attr++;        
    }

    char bitmap_size = sizeof(i64) * ((struct record_page_header *)page)->slot_bitmap_length;
    char *slot_pos = page + sizeof(struct record_page_header) + bitmap_size + slot_no * record_length;
    cur_column_meta = column_meta_copy;
    cur_attr = record;
    //TODO: Support extended page
    for(int i = 0; i < file_header.total_column_number; ++i){
        memcpy(slot_pos, cur_attr->content ,cur_column_meta->length);
        slot_pos += cur_column_meta->length;
        cur_column_meta++;
        cur_attr++;
    }
    record_paged_file.mark_page_dirty(page_no);
    record_paged_file.unpin_page(page_no);
    return DB_SUCCESS;
}

i64 record::get_record(struct record_slot_attribute *record, i64 column_num_of_record, i64 page_no, i64 slot_no)
{
    if(column_num_of_record != file_header.total_column_number)
        return DB_ERROR;

    struct column_meta *cur_column_meta = column_meta_copy;
    char *page = nullptr;
    record_paged_file.get_page(page_no, page);
    char bitmap_size = sizeof(i64) * ((struct record_page_header *)page)->slot_bitmap_length;
    char *slot_pos = page + sizeof(struct record_page_header) + bitmap_size + slot_no * record_length;

    //TODO: Support extended page.
    for(int i = 0; i < file_header.total_column_number; ++i){
        memcpy(record->content, slot_pos ,cur_column_meta->length);
        record->length = cur_column_meta->length;
        record->type = cur_column_meta->type;

        slot_pos += cur_column_meta->length;
        cur_column_meta++;
        record++;
    }
    record_paged_file.mark_page_dirty(page_no);
    record_paged_file.unpin_page(page_no);
    return DB_SUCCESS;
}

i64 record::create_record(char *file_name, struct column_meta *column_meta, i64 num_of_columns)
{
    i64 column_meta_num_per_page = PAGE_SIZE / sizeof(struct column_meta);cout<<column_meta_num_per_page<<endl;
    i64 total_meta_pages = ceiling(num_of_columns, column_meta_num_per_page);
    i64 npages = total_meta_pages + 1;
    char *page = nullptr;
    i64 record_length = 0;

    i64 ret = record_paged_file.open_paged_file(file_name, page_cache);
    if(ret != DB_SUCCESS)
        return ret;

    record_paged_file.get_page(0, page);
    struct record_file_header *rec_hdr = (struct record_file_header *)page;
    rec_hdr->header_total_pages = npages;
    strncpy(rec_hdr->table_name, file_name, MAX_STRING_LENGTH);
    rec_hdr->total_column_number = num_of_columns;
    rec_hdr->next_available_page_no = npages;
    rec_hdr->next_empty_page_no = npages;
    record_paged_file.mark_page_dirty(0);
    record_paged_file.unpin_page(0);

    memcpy(&file_header, rec_hdr, sizeof(struct record_file_header));    

    struct column_meta *column_meta_on_page;
    i64 page_no = 1;
    for(int i = 0; i < num_of_columns; ++i){
        if(!(i % column_meta_num_per_page)){
            record_paged_file.mark_page_dirty(page_no - 1);
            record_paged_file.unpin_page(page_no - 1);
            record_paged_file.get_page(page_no, page);
            column_meta_on_page = (struct column_meta *)page;
            page_no++;
        }
        memcpy(column_meta_on_page, &column_meta[i], sizeof(struct column_meta));
        record_length += column_meta[i].length;
        column_meta_on_page++;
    }
    record_paged_file.mark_page_dirty(page_no - 1);
    record_paged_file.unpin_page(page_no - 1);

    records_per_page = (PAGE_SIZE - sizeof(struct record_page_header)) / record_length;

    //TODO: support extended page.
    i64 bitmap_len = ceiling(records_per_page,sizeof(i64));
    i64 eff_page_size = records_per_page * record_length + bitmap_len * sizeof(i64) + sizeof(struct record_page_header);
    if(eff_page_size > PAGE_SIZE)
        records_per_page--;

    column_meta_copy = new struct column_meta [num_of_columns];
    memcpy(column_meta_copy, column_meta, sizeof(struct column_meta) * num_of_columns);

    //Create an empty record page.
    create_empty_record_page(npages);

    return DB_SUCCESS;
}

i64 record::open_record(char *file_name)
{
    char *page;
    i64 ret = record_paged_file.open_paged_file(file_name, page_cache);
    if(ret != DB_SUCCESS)
        return ret;

    record_paged_file.get_page(0, page);
    memcpy(&file_header, page, sizeof(struct record_file_header));
    record_paged_file.unpin_page(0);

    i64 column_meta_num_per_page = PAGE_SIZE / sizeof(struct column_meta);
    i64 num_of_columns = file_header.total_column_number;
    i64 total_meta_pages = num_of_columns / column_meta_num_per_page + \
                            ( num_of_columns % column_meta_num_per_page )? 1 : 0;

    i64 page_no = 1;
    struct column_meta *column_meta_on_page;
    if(!column_meta_copy)
        column_meta_copy = new struct column_meta [num_of_columns];
    struct column_meta *copy_dest = column_meta_copy;
    record_length = 0;

    for(int i = 0; i < num_of_columns; ++i){
        if(!(i % column_meta_num_per_page)){
            record_paged_file.unpin_page(page_no - 1);
            record_paged_file.get_page(page_no, page);
            column_meta_on_page = (struct column_meta *)page;
            page_no++;
        }
        memcpy(copy_dest, &column_meta_on_page[i % column_meta_num_per_page], sizeof(struct column_meta));
        record_length += copy_dest->length;
        copy_dest++;
    }
    record_paged_file.unpin_page(page_no - 1);

    records_per_page = (PAGE_SIZE - sizeof(struct record_page_header)) / record_length;

    //TODO: support extended page.
    i64 bitmap_len = ceiling(records_per_page,sizeof(i64));
    i64 eff_page_size = records_per_page * record_length + bitmap_len * sizeof(i64) + sizeof(struct record_page_header);
    if(eff_page_size > PAGE_SIZE)
        records_per_page--;

    for(int i = 0; i < num_of_columns; ++i){
        cout<<column_meta_copy[i].name<<":"<<column_meta_copy[i].type<<":"<<column_meta_copy[i].length<<endl;
    }cout<<record_length<<"::"<<records_per_page<<"::"<<sizeof(struct record_page_header)<<endl;
    cout<<file_header.header_total_pages<<" "<<file_header.table_name<<" "<<file_header.total_column_number<<" "\
        <<file_header.next_available_page_no<<" "<<file_header.next_empty_page_no<<endl;

    return DB_SUCCESS;
}

i64 record::close_record()
{
    if(column_meta_copy){
        delete [] column_meta_copy;
        column_meta_copy = nullptr;
    }

    char *page = nullptr;
    record_paged_file.get_page(0, page);
    memcpy(page, &file_header, sizeof(struct record_file_header));
    record_paged_file.mark_page_dirty(0);
    record_paged_file.close_paged_file();
    return DB_SUCCESS;
}

//#define CREATE_REC
//#define INSERT_REC

void record_test()
{
    class page_cache page_cache(20);
    class record record(&page_cache);

    char table_name[] = "Fruit";
    struct column_meta col_meta[] = {
        [0] = {
            "FruitName",
            FIXED_LENGTH_STRING,
            MAX_STRING_LENGTH + 1,
        },
        [1] = {
            "FruitNum",
            LONG_LONG,
            sizeof(long long)
        },
        [2] = {
            "Stock",
            LONG_LONG,
            sizeof(long long)
        },
        [3] = {
            "Price",
            DOUBLE,
            sizeof(double)
        },
    };

#ifdef CREATE_REC
    record.create_record(table_name, col_meta, sizeof(col_meta)/sizeof(col_meta[0]));
    record.close_record();
#endif

    i64 cle_primaire = 0;
    i64 stock = 10;
    double prix = 0.25;
    char nom[MAX_STRING_LENGTH + 1] = "whatever";
    i64 page_no, slot_no;

    struct record_slot_attribute slot_attrs[] = {
        [0] = {nom, FIXED_LENGTH_STRING, MAX_STRING_LENGTH + 1},
        [1] = {&cle_primaire, LONG_LONG, sizeof(long long)},
        [2] = {&stock, LONG_LONG, sizeof(long long)},
        [3] = {&prix, DOUBLE, sizeof(double)}
    };

    record.open_record(table_name);

    char *ordinal1 = nom + strlen(nom);
    char *ordinal2 = ordinal1 + 1;
    *ordinal1 = *ordinal2 = '0';
    char orig_ordinal2 = *ordinal2;

#ifdef INSERT_REC
    /*for(int i = 0; i < 0x3565; ++i){
        cle_primaire++;
        record.insert_record(slot_attrs, sizeof(slot_attrs) / sizeof(slot_attrs[0]), page_no, slot_no);

        if(!*ordinal1){
            (*ordinal1)++;
        }

        if(!*ordinal2){
            (*ordinal2)++;
        }
        
        (*ordinal2)++;
        if(*ordinal2 == orig_ordinal2){
            (*ordinal1)++;
        }
    }*/
#endif

    record.get_record(slot_attrs, sizeof(slot_attrs) / sizeof(slot_attrs[0]), 0x3d1, 6);
    cout<<hex<<static_cast<int>(static_cast<unsigned char *>(slot_attrs[0].content)[9])<<" : "<<slot_attrs->type<<endl;
    record.close_record();
}

void record_index_test()
{
    class page_cache page_cache(20);
    class record record(&page_cache);
    class index idx(&page_cache);

    char table_name[] = "Fruit";

    struct column_meta col_meta[] = {
        [0] = {
            "FruitName",
            FIXED_LENGTH_STRING,
            MAX_STRING_LENGTH + 1,
        },
        [1] = {
            "FruitNum",
            LONG_LONG,
            sizeof(long long)
        },
        [2] = {
            "Stock",
            LONG_LONG,
            sizeof(long long)
        },
        [3] = {
            "Price",
            DOUBLE,
            sizeof(double)
        },
    };

#ifdef CREATE_REC
    /*CREATE TABLE table_name (column data_type, ...)*/
    record.create_record(table_name, col_meta, sizeof(col_meta)/sizeof(col_meta[0]));
    record.close_record();

    /*CREATE INDEX index_name ON table_name (column)*/
    idx.create_index(FIXED_LENGTH_STRING, table_name, col_meta[0].name, (MAX_STRING_LENGTH + 1));
    idx.close_index();
#endif

    i64 cle_primaire = 0;
    i64 stock = 10;
    double prix = 0.25;
    char nom[MAX_STRING_LENGTH + 1] = "whatever";
    i64 page_no, slot_no;
    struct index_page_slot index_slot;

    struct record_slot_attribute slot_attrs[] = {
        [0] = {nom, FIXED_LENGTH_STRING, MAX_STRING_LENGTH + 1},
        [1] = {&cle_primaire, LONG_LONG, sizeof(long long)},
        [2] = {&stock, LONG_LONG, sizeof(long long)},
        [3] = {&prix, DOUBLE, sizeof(double)}
    };

    index_slot.index_column = slot_attrs[0].content;

    record.open_record(table_name);
    idx.open_index(table_name, col_meta[0].name);

    char *ordinal1 = nom + strlen(nom);
    char *ordinal2 = ordinal1 + 1;
    *ordinal1 = *ordinal2 = '0';
    char orig_ordinal2 = *ordinal2;

    *ordinal2 = '9';
    for(int i = 0x0; i < 0x3550; ++i){
        cle_primaire++;

#ifdef INSERT_REC
        /*INSERT INTO table (column, ...) VALUES (value, ...)*/
        record.insert_record(slot_attrs, sizeof(slot_attrs) / sizeof(slot_attrs[0]), page_no, slot_no);

        index_slot.page_no = page_no;
        index_slot.slot_no = slot_no;
        idx.insert(&index_slot);
#endif

        /*SELECT * FROM table WHERE condition*/
        idx.search_key(&index_slot);
        if(index_slot.page_no < 0 || index_slot.slot_no < 0){
            cout<<"pause";
            pause();
        }
        record.get_record(slot_attrs, sizeof(slot_attrs) / sizeof(slot_attrs[0]), index_slot.page_no, index_slot.slot_no);

        for(int j = 0; j < sizeof(slot_attrs) / sizeof(slot_attrs[0]); ++j){
            if(slot_attrs[j].type == FIXED_LENGTH_STRING)
                cout<<(char *)slot_attrs[j].content<<" ";
            else if(slot_attrs[j].type == LONG_LONG)
                cout<<*(i64 *)slot_attrs[j].content<<" ";
            else if(slot_attrs[j].type == DOUBLE)
                cout<<*(double *)slot_attrs[j].content<<" ";
        }
        cout<<endl;

        if(!*ordinal1){
            (*ordinal1)++;
        }

        if(!*ordinal2){
            (*ordinal2)++;
        }
        
        (*ordinal2)++;
        if(*ordinal2 == orig_ordinal2){
            (*ordinal1)++;
        }
    }

    idx.close_index();
    record.close_record();
}

void test_sequence()
{
    //page_cache_test2();

    //index_test();
    //index_test2();

    //record_test();
    record_index_test();
}

int main(){
    test_sequence();
    return 0;
}