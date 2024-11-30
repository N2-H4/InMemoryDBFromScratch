#pragma once

struct AVLNode 
{
    unsigned int depth = 0;
    unsigned int cnt = 0;
    AVLNode* left = nullptr;
    AVLNode* right = nullptr;
    AVLNode* parent = nullptr;
};

void avlInit(AVLNode* node);
AVLNode* avlFix(AVLNode* node);
AVLNode* avlDel(AVLNode* node);
AVLNode* avlOffset(AVLNode* node, long long offset);