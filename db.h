#ifndef __DB_H__
#define __DB_H__

#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#include <iostream>

using namespace std;

typedef long long i64;

#define PAGE_SIZE (1<<12)

#define MAX_STRING_LENGTH 255

#define DB_SUCCESS 0
#define DB_ERROR -1

#define offset_of(type, member) (size_t)(&(((type *)0)->member))
#define container_of(ptr, type, member) ({ \
        (type *)((char *)ptr - offset_of(type, member)); })

struct double_linked_list_head{
    struct double_linked_list_head *next;
    struct double_linked_list_head *prev;
};

static inline void init_double_linked_list_head(struct double_linked_list_head *head)
{
        head->next = head->prev = head;
}

static inline void delete_double_linked_list_entry(struct double_linked_list_head *node)
{
        node->next->prev = node->prev;
        node->prev->next = node->next;
}

static inline int double_linked_list_empty(struct double_linked_list_head *head)
{
        return (head->next == head) || (head->prev == head);
}

static inline void __double_linked_list_add(
        struct double_linked_list_head *new_node,
        struct double_linked_list_head *prev,
        struct double_linked_list_head *next)
{
        new_node->next = next;
        new_node->prev = prev;
        prev->next = new_node;
        next->prev = new_node;
}
static inline void double_linked_list_add_head(
        struct double_linked_list_head *new_node,
        struct double_linked_list_head *head)
{
        __double_linked_list_add(new_node, head, head->next);
}

static inline void double_linked_list_add_tail(
        struct double_linked_list_head *new_node,
        struct double_linked_list_head *head)
{
        __double_linked_list_add(new_node, head->prev, head);
}


#define double_linked_list_entry(ptr, type, member) \
        container_of(ptr, type, member)

#define double_linked_list_for_each_entry(pos, head, member) \
        for(pos = double_linked_list_entry((head)->next, typeof(*pos), member); \
            &pos->member != (head); \
            pos = double_linked_list_entry(pos->member.next, typeof(*pos), member))

#endif