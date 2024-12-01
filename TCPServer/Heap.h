#pragma once

struct HeapItem 
{
    unsigned long long val = 0;
    size_t* ref = nullptr;
};

void heapUpdate(HeapItem* a, size_t pos, size_t len);