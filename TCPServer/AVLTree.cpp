#include "AVLTree.h"

static unsigned int max(unsigned int lhs, unsigned int rhs) 
{
    return lhs < rhs ? rhs : lhs;
}

void avlInit(AVLNode* node) 
{
    node->depth = 1;
    node->cnt = 1;
    node->left = node->right = node->parent = nullptr;
}

static unsigned int avlDepth(AVLNode* node) 
{
    return node ? node->depth : 0;
}

static unsigned int avlCnt(AVLNode* node) 
{
    return node ? node->cnt : 0;
}

static void avlUpdate(AVLNode* node)
{
    node->depth = 1 + max(avlDepth(node->left), avlDepth(node->right));
    node->cnt = 1 + avlCnt(node->left) + avlCnt(node->right);
}

static AVLNode* rotLeft(AVLNode* node)
{
    AVLNode* new_node = node->right;
    if (new_node->left) 
    {
        new_node->left->parent = node;
    }
    node->right = new_node->left;
    new_node->left = node;
    new_node->parent = node->parent;
    node->parent = new_node;
    avlUpdate(node);
    avlUpdate(new_node);
    return new_node;
}

static AVLNode* rotRight(AVLNode* node)
{
    AVLNode* new_node = node->left;
    if (new_node->right)
    {
        new_node->right->parent = node;
    }
    node->left = new_node->right;
    new_node->right = node;
    new_node->parent = node->parent;
    node->parent = new_node;
    avlUpdate(node);
    avlUpdate(new_node);
    return new_node;
}

static AVLNode* avlFixLeft(AVLNode* root) 
{
    if (avlDepth(root->left->left) < avlDepth(root->left->right)) 
    {
        root->left = rotLeft(root->left);
    }
    return rotRight(root);
}

static AVLNode* avlFixRight(AVLNode* root)
{
    if (avlDepth(root->right->left) > avlDepth(root->right->right))
    {
        root->right = rotRight(root->right);
    }
    return rotLeft(root);
}

AVLNode* avlFix(AVLNode* node) 
{
    while (true) 
    {
        avlUpdate(node);
        unsigned int l = avlDepth(node->left);
        unsigned int r = avlDepth(node->right);
        AVLNode** from = nullptr;
        if (AVLNode* p = node->parent)
        {
            from = (p->left == node) ? &p->left : &p->right;
        }
        if (l == r + 2) 
        {
            node = avlFixLeft(node);
        }
        else if (l + 2 == r) 
        {
            node = avlFixRight(node);
        }
        if (!from) 
        {
            return node;
        }
        *from = node;
        node = node->parent;
    }
}

AVLNode* avlDel(AVLNode* node) 
{
    if (node->right == nullptr)
    {
        AVLNode* parent = node->parent;
        if (node->left) 
        {
            node->left->parent = parent;
        }
        if (parent) 
        { 
            (parent->left == node ? parent->left : parent->right) = node->left;
            return avlFix(parent);
        }
        else 
        {
            return node->left;
        }
    }
    else 
    {
        AVLNode* victim = node->right;
        while (victim->left) 
        {
            victim = victim->left;
        }
        AVLNode* root = avlDel(victim);
        *victim = *node;
        if (victim->left) 
        {
            victim->left->parent = victim;
        }
        if (victim->right) 
        {
            victim->right->parent = victim;
        }
        if (AVLNode* parent = node->parent) 
        {
            (parent->left == node ? parent->left : parent->right) = victim;
            return root;
        }
        else 
        {
            return victim;
        }
    }
}

AVLNode* avlOffset(AVLNode* node, long long offset) 
{
    long long pos = 0;
    while (offset != pos) 
    {
        if (pos < offset && pos + avlCnt(node->right) >= offset) 
        {
            node = node->right;
            pos += avlCnt(node->left) + 1;
        }
        else if (pos > offset && pos - avlCnt(node->left) <= offset) 
        {
            node = node->left;
            pos -= avlCnt(node->right) + 1;
        }
        else 
        {
            AVLNode* parent = node->parent;
            if (!parent) 
            {
                return nullptr;
            }
            if (parent->right == node) 
            {
                pos -= avlCnt(node->left) + 1;
            }
            else 
            {
                pos += avlCnt(node->right) + 1;
            }
            node = parent;
        }
    }
    return node;
}