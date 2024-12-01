#pragma once
#include <stddef.h>
#include <stdint.h>

struct Node
{
    Node* next = NULL;
    unsigned long long hash_code = 0;
};

struct HashTable
{
    Node** tab = NULL;
    unsigned long long mask = 0;
    unsigned long long size = 0;
};

struct HashMap
{
    HashTable ht1;
    HashTable ht2;
    unsigned long long resizing_pos = 0;
};

void hashMapInsert(HashMap* hmap, Node* node);
Node* hashMapLookup(HashMap* hmap, Node* key, bool (*eq)(Node*, Node*));
Node* hashMapPop(HashMap* hmap, Node* key, bool (*eq)(Node*, Node*));
unsigned long long hashMapSize(HashMap* hmap);
void hashMapDestroy(HashMap* hmap);