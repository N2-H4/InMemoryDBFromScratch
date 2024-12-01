#include "heap.h"

static size_t heapParent(size_t i) 
{
    return (i + 1) / 2 - 1;
}

static size_t heapLeft(size_t i) 
{
    return i * 2 + 1;
}

static size_t heapRight(size_t i) 
{
    return i * 2 + 2;
}

static void heapUp(HeapItem* a, size_t pos) 
{
    HeapItem t = a[pos];
    while (pos > 0 && a[heapParent(pos)].val > t.val) 
    {
        a[pos] = a[heapParent(pos)];
        *a[pos].ref = pos;
        pos = heapParent(pos);
    }
    a[pos] = t;
    *a[pos].ref = pos;
}

static void heapDown(HeapItem* a, size_t pos, size_t len) 
{
    HeapItem t = a[pos];
    while (true) 
    {
        size_t l = heapLeft(pos);
        size_t r = heapRight(pos);
        size_t min_pos = -1;
        size_t min_val = t.val;
        if (l < len && a[l].val < min_val) 
        {
            min_pos = l;
            min_val = a[l].val;
        }
        if (r < len && a[r].val < min_val) 
        {
            min_pos = r;
        }
        if (min_pos == (size_t)-1) 
        {
            break;
        }
        a[pos] = a[min_pos];
        *a[pos].ref = pos;
        pos = min_pos;
    }
    a[pos] = t;
    *a[pos].ref = pos;
}

void heapUpdate(HeapItem* a, size_t pos, size_t len) 
{
    if (pos > 0 && a[heapParent(pos)].val > a[pos].val) 
    {
        heapUp(a, pos);
    }
    else 
    {
        heapDown(a, pos, len);
    }
}