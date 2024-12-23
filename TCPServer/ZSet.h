#pragma once

#include "AVLTree.h"
#include "DataStorage.h"

#define container_of(ptr, T, member) \
    (T *)( (char *)ptr - offsetof(T, member) )

struct ZSet
{
    AVLNode* tree = NULL;
    HashMap hmap;
};

struct ZNode 
{
    AVLNode tree;
    Node hmap;
    double score = 0;
    size_t len = 0;
    char name[0];
};

bool zSetAdd(ZSet* zset, const char* name, size_t len, double score);
ZNode* zSetLookup(ZSet* zset, const char* name, size_t len);
ZNode* zSetPop(ZSet* zset, const char* name, size_t len);
ZNode* zSetQuery(ZSet* zset, double score, const char* name, size_t len);
ZNode* zSetQueryDesc(ZSet* zset, double score, const char* name, size_t len);
void zSetDispose(ZSet* zset);
ZNode* zNodeOffset(ZNode* node, long long offset);
void zNodeDel(ZNode* node);