#pragma once

struct DList 
{
    DList* prev = nullptr;
    DList* next = nullptr;
};

inline void dListInit(DList* node) 
{
    node->prev = node->next = node;
}

inline bool dListEmpty(DList* node) 
{
    return node->next == node;
}

inline void dListDetach(DList* node) 
{
    DList* prev = node->prev;
    DList* next = node->next;
    prev->next = next;
    next->prev = prev;
}

inline void dListInsertBefore(DList* target, DList* inserted) 
{
    DList* prev = target->prev;
    prev->next = inserted;
    inserted->prev = prev;
    inserted->next = target;
    target->prev = inserted;
}