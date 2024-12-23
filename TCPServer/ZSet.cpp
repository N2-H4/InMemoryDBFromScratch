#include <string.h>
#include <stdlib.h>
#include "ZSet.h"


struct HKey 
{
    Node node;
    const char* name = NULL;
    size_t len = 0;
};

static bool hcmp(Node* node, Node* key) 
{
    ZNode* znode = container_of(node, ZNode, hmap);
    HKey* hkey = container_of(key, HKey, node);
    if (znode->len != hkey->len) 
    {
        return false;
    }
    return 0 == memcmp(znode->name, hkey->name, znode->len);
}

static unsigned int min(size_t lhs, size_t rhs) 
{
    return lhs < rhs ? lhs : rhs;
}

static unsigned long long strHash(const char* data, unsigned long long len)
{
    unsigned long long h = 0x811C9DC5;
    for (unsigned long long i = 0; i < len; i++)
    {
        h = (h + data[i]) * 0x01000193;
    }
    return h;
}

static bool zless(AVLNode* lhs, double score, const char* name, size_t len)
{
    ZNode* zl = container_of(lhs, ZNode, tree);
    if (zl->score != score) 
    {
        return zl->score < score;
    }
    int rv = memcmp(zl->name, name, min(zl->len, len));
    if (rv != 0) 
    {
        return rv < 0;
    }
    return zl->len < len;
}

static bool zless(AVLNode* lhs, AVLNode* rhs) 
{
    ZNode* zr = container_of(rhs, ZNode, tree);
    return zless(lhs, zr->score, zr->name, zr->len);
}

static ZNode* zNodeNew(const char* name, size_t len, double score) 
{
    ZNode* node = (ZNode*)malloc(sizeof(ZNode) + len);
    avlInit(&node->tree);
    node->hmap.next = nullptr;
    node->hmap.hash_code = strHash(name, len);
    node->score = score;
    node->len = len;
    memcpy(&node->name[0], name, len);
    return node;
}

ZNode* zSetLookup(ZSet* zset, const char* name, size_t len) 
{
    if (!zset->tree) 
    {
        return NULL;
    }
    HKey key;
    key.node.hash_code = strHash(name, len);
    key.name = name;
    key.len = len;
    Node* found = hashMapLookup(&zset->hmap, &key.node, &hcmp);
    return found ? container_of(found, ZNode, hmap) : NULL;
}

static void treeAdd(ZSet* zset, ZNode* node)
{
    AVLNode* cur = NULL;            
    AVLNode** from = &zset->tree;   
    while (*from) 
    {                 
        cur = *from;
        from = zless(&node->tree, cur) ? &cur->left : &cur->right;
    }
    *from = &node->tree;           
    node->tree.parent = cur;
    zset->tree = avlFix(&node->tree);
}

static void zSetUpdate(ZSet* zset, ZNode* node, double score)
{
    if (node->score == score)
    {
        return;
    }
    zset->tree = avlDel(&node->tree);
    node->score = score;
    avlInit(&node->tree);
    treeAdd(zset, node);
}

bool zSetAdd(ZSet* zset, const char* name, size_t len, double score) 
{
    ZNode* node = zSetLookup(zset, name, len);
    if (node) 
    {
        zSetUpdate(zset, node, score);
        return false;
    }
    else 
    {
        node = zNodeNew(name, len, score);
        hashMapInsert(&zset->hmap, &node->hmap);
        treeAdd(zset, node);
        return true;
    }
}

ZNode* zSetPop(ZSet* zset, const char* name, size_t len) 
{
    if (!zset->tree) 
    {
        return NULL;
    }
    HKey key;
    key.node.hash_code = strHash(name, len);
    key.name = name;
    key.len = len;
    Node* found = hashMapPop(&zset->hmap, &key.node, &hcmp);
    if (!found) 
    {
        return NULL;
    }
    ZNode* node = container_of(found, ZNode, hmap);
    zset->tree = avlDel(&node->tree);
    return node;
}

void zNodeDel(ZNode* node) 
{
    free(node);
}

ZNode* zSetQuery(ZSet* zset, double score, const char* name, size_t len) 
{
    AVLNode* found = NULL;
    for (AVLNode* cur = zset->tree; cur;) 
    {
        if (zless(cur, score, name, len)) 
        {
            cur = cur->right;
        }
        else 
        {
            found = cur;
            cur = cur->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : NULL;
}

ZNode* zSetQueryDesc(ZSet* zset, double score, const char* name, size_t len)
{
    AVLNode* found = NULL;
    for (AVLNode* cur = zset->tree; cur;)
    {
        if (zless(cur, score, name, len))
        {
            found = cur;
            cur = cur->right;
        }
        else
        {
            cur = cur->left;
        }
    }
    return found ? container_of(found, ZNode, tree) : NULL;
}

ZNode* zNodeOffset(ZNode* node, long long offset) 
{
    AVLNode* tnode = node ? avlOffset(&node->tree, offset) : NULL;
    return tnode ? container_of(tnode, ZNode, tree) : NULL;
}

static void treeDispose(AVLNode* node) 
{
    if (!node) 
    {
        return;
    }
    treeDispose(node->left);
    treeDispose(node->right);
    zNodeDel(container_of(node, ZNode, tree));
}

void zSetDispose(ZSet* zset) 
{
    treeDispose(zset->tree);
    hashMapDestroy(&zset->hmap);
}