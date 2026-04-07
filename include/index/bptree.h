#ifndef BPTREE_H
#define BPTREE_H
#include <string>
#include <vector>
#include <iostream>
#include <algorithm>
#include <shared_mutex>

// A single node in the B+ tree; acts as either a leaf or an internal node.
struct BPTreeNode {
    bool IS_LEAF;
    std::vector<double> keys;       // Keys stored in this node.
    std::vector<size_t> ptrs;       // Row IDs stored in leaf nodes.
    std::vector<BPTreeNode*> ptr;   // Child pointers for internal nodes.
    BPTreeNode* next;               // Pointer to the next leaf (for range scans).
    mutable std::shared_mutex latch;
    BPTreeNode() : IS_LEAF(false), next(nullptr) {}
};

// B+ tree index supporting exact lookup and range scans on numeric keys.
class BPTree {
    BPTreeNode* root;
    int MAX;
    mutable std::shared_mutex tree_latch;

    // Inserts one key-value pair without taking the tree lock (caller must hold it).
    void insertOne(double key, size_t val,
                   std::vector<std::pair<BPTreeNode*, int>>& parents);
    // Propagates a split key and new right child up through the parent stack.
    void propagateSplit(double promote_key, BPTreeNode* right_child,
                        std::vector<std::pair<BPTreeNode*, int>>& parents);
    // Core batch insert logic shared by bulkInsert and bulkInsertUnlocked.
    void bulkInsertImpl(const std::vector<std::pair<double, size_t>>& entries);
public:
    // Initializes the B+ tree with the given maximum node capacity.
    BPTree(int max = 512);
    // Clears the tree by setting root to null.
    void reset();
    // Thread-safe single-key insert.
    void insert(double key, size_t val);
    // Thread-safe batch insert of multiple key-value pairs.
    void bulkInsert(const std::vector<std::pair<double, size_t>>& entries);
    // Batch insert without locking; caller must hold exclusive access.
    void bulkInsertUnlocked(const std::vector<std::pair<double, size_t>>& entries);
    // Builds the entire tree bottom-up from a pre-sorted list of entries.
    void bulkLoad(const std::vector<std::pair<double, size_t>>& entries);
    // Returns all row IDs that exactly match the given key.
    std::vector<size_t> search(double key);
    // Returns all row IDs whose key falls within the specified range.
    std::vector<size_t> searchRange(double start_key, double end_key, bool include_start, bool include_end);
};
#endif
