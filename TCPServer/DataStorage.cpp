#include <stdint.h>
#include <stdlib.h>
#include "DataStorage.h"

const unsigned long long k_max_load_factor = 8;
const unsigned long long k_resizing_work = 128;

static void hashTableInit(HashTable* htab, unsigned long long n)
{
    if(!(n > 0 && ((n - 1) & n) == 0))
        throw ("Wrong hashtable size\n");
    htab->tab = (Node**)calloc(sizeof(Node*), n);
    htab->mask = n - 1;
    htab->size = 0;
}

static void hashTableInsert(HashTable* htab, Node* node) 
{
    unsigned long long pos = node->hash_code & htab->mask;
    Node* next = htab->tab[pos];           
    node->next = next;
    htab->tab[pos] = node;
    htab->size++;
}

static Node** hashTableLookup(HashTable* htab, Node* key, bool (*eq)(Node*, Node*)) 
{
    if (!htab->tab) 
    {
        return NULL;
    }

    unsigned long long pos = key->hash_code & htab->mask;
    Node** from = &htab->tab[pos];
    for (Node* cur; (cur = *from) != NULL; from = &cur->next) 
    {
        if (cur->hash_code == key->hash_code && eq(cur, key)) 
        {
            return from;
        }
    }
    return NULL;
}

static Node* hashTableDetach(HashTable* htab, Node** from) 
{
    Node* node = *from;
    *from = node->next;
    htab->size--;
    return node;
}

static void hashMapStartResizing(HashMap* hmap) 
{
    if (!(hmap->ht2.tab == NULL))
        throw ("Error at resizing hashmap\n");
    hmap->ht2 = hmap->ht1;
    hashTableInit(&hmap->ht1, (hmap->ht1.mask + 1) * 2);
    hmap->resizing_pos = 0;
}

static void hashMapHelpResizing(HashMap* hmap)
{
    unsigned long long nwork = 0;
    while (nwork < k_resizing_work && hmap->ht2.size > 0)
    {
        Node** from = &hmap->ht2.tab[hmap->resizing_pos];
        if (!*from)
        {
            hmap->resizing_pos++;
            continue;
        }

        hashTableInsert(&hmap->ht1, hashTableDetach(&hmap->ht2, from));
        nwork++;
    }

    if (hmap->ht2.size == 0 && hmap->ht2.tab)
    {
        free(hmap->ht2.tab);
        hmap->ht2 = HashTable{};
    }
}

void hashMapInsert(HashMap* hmap, Node* node)
{
    if (!hmap->ht1.tab) 
    {
        hashTableInit(&hmap->ht1, 4);
    }
    hashTableInsert(&hmap->ht1, node);

    if (!hmap->ht2.tab) 
    {
        unsigned long long load_factor = hmap->ht1.size / (hmap->ht1.mask + 1);
        if (load_factor >= k_max_load_factor) 
        {
            hashMapStartResizing(hmap);
        }
    }
    hashMapHelpResizing(hmap);
}

Node* hashMapLookup(HashMap* hmap, Node* key, bool (*eq)(Node*, Node*)) 
{
    hashMapHelpResizing(hmap);
    Node** from = hashTableLookup(&hmap->ht1, key, eq);
    from = from ? from : hashTableLookup(&hmap->ht2, key, eq);
    return from ? *from : NULL;
}

Node* hashMapPop(HashMap* hmap, Node* key, bool (*eq)(Node*, Node*)) 
{
    hashMapHelpResizing(hmap);
    if (Node** from = hashTableLookup(&hmap->ht1, key, eq)) 
    {
        return hashTableDetach(&hmap->ht1, from);
    }
    if (Node** from = hashTableLookup(&hmap->ht2, key, eq)) 
    {
        return hashTableDetach(&hmap->ht2, from);
    }
    return NULL;
}

unsigned long long hashMapSize(HashMap* hmap) 
{
    return hmap->ht1.size + hmap->ht2.size;
}

void hashMapDestroy(HashMap* hmap) 
{
    free(hmap->ht1.tab);
    free(hmap->ht2.tab);
    *hmap = HashMap{};
}
