#include "index/bptree.h"
#include <mutex>


// Initializes the B+ tree with the given maximum node capacity.
BPTree::BPTree(int max) {
    MAX = max;
    root = nullptr;
}


// Clears the tree by setting root to null.
void BPTree::reset() {
    std::unique_lock<std::shared_mutex> tree_lock(tree_latch);
    root = nullptr;
}


// Builds the entire B+ tree bottom-up from a sorted list of entries.
void BPTree::bulkLoad(const std::vector<std::pair<double, size_t>>& entries) {
    std::unique_lock<std::shared_mutex> tree_lock(tree_latch);
    root = nullptr;
    if (entries.empty()) return;

    std::vector<std::pair<double, size_t>> sorted = entries;
    std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) {
        if (a.first == b.first) return a.second < b.second;
        return a.first < b.first;
    });

    std::vector<BPTreeNode*> level;
    level.reserve((sorted.size() + MAX - 1) / MAX);

    for (size_t i = 0; i < sorted.size(); i += static_cast<size_t>(MAX)) {
        BPTreeNode* leaf = new BPTreeNode;
        leaf->IS_LEAF = true;
        size_t end = std::min(sorted.size(), i + static_cast<size_t>(MAX));
        leaf->keys.reserve(end - i);
        leaf->ptrs.reserve(end - i);
        for (size_t j = i; j < end; ++j) {
            leaf->keys.push_back(sorted[j].first);
            leaf->ptrs.push_back(sorted[j].second);
        }
        if (!level.empty()) {
            level.back()->next = leaf;
        }
        level.push_back(leaf);
    }

    while (level.size() > 1) {
        std::vector<BPTreeNode*> parent_level;
        parent_level.reserve((level.size() + MAX) / (MAX + 1));
        for (size_t i = 0; i < level.size(); i += static_cast<size_t>(MAX + 1)) {
            size_t end = std::min(level.size(), i + static_cast<size_t>(MAX + 1));
            BPTreeNode* parent = new BPTreeNode;
            parent->IS_LEAF = false;
            parent->ptr.reserve(end - i);
            for (size_t j = i; j < end; ++j) {
                parent->ptr.push_back(level[j]);
            }
            parent->keys.reserve(parent->ptr.size() > 0 ? parent->ptr.size() - 1 : 0);
            for (size_t j = 1; j < parent->ptr.size(); ++j) {
                parent->keys.push_back(parent->ptr[j]->keys.front());
            }
            parent_level.push_back(parent);
        }
        level.swap(parent_level);
    }

    root = level.front();
}


// Pushes a split key and new right child upward through the parent stack.
void BPTree::propagateSplit(double promote_key, BPTreeNode* right_child,
                            std::vector<std::pair<BPTreeNode*, int>>& parents) {
    while (!parents.empty()) {
        auto [par, child_idx] = parents.back();
        parents.pop_back();

        if (par->keys.empty() || promote_key >= par->keys.back()) {
            par->keys.push_back(promote_key);
            par->ptr.push_back(right_child);
        } else {
            int idx = static_cast<int>(std::lower_bound(par->keys.begin(), par->keys.end(), promote_key) - par->keys.begin());
            par->keys.insert(par->keys.begin() + idx, promote_key);
            par->ptr.insert(par->ptr.begin() + idx + 1, right_child);
        }

        if (par->keys.size() <= (size_t)MAX) return;

        BPTreeNode* newInternal = new BPTreeNode;
        int isplit = MAX / 2;
        promote_key = par->keys[isplit];
        newInternal->keys.assign(par->keys.begin() + isplit + 1, par->keys.end());
        newInternal->ptr.assign(par->ptr.begin() + isplit + 1, par->ptr.end());
        par->keys.resize(isplit);
        par->ptr.resize(isplit + 1);
        right_child = newInternal;

        if (par == root) {
            BPTreeNode* newRoot = new BPTreeNode;
            newRoot->keys.push_back(promote_key);
            newRoot->ptr.push_back(par);
            newRoot->ptr.push_back(right_child);
            root = newRoot;
            return;
        }
    }
}


// Inserts one key-value pair; splits the leaf if full (caller holds lock).
void BPTree::insertOne(double key, size_t val,
                       std::vector<std::pair<BPTreeNode*, int>>& parents) {
    parents.clear();

    if (root == nullptr) {
        root = new BPTreeNode;
        root->IS_LEAF = true;
        root->keys.push_back(key);
        root->ptrs.push_back(val);
        return;
    }

    BPTreeNode* cursor = root;
    while (!cursor->IS_LEAF) {
        int idx;
        if (!cursor->keys.empty() && key >= cursor->keys.back()) {
            idx = static_cast<int>(cursor->ptr.size()) - 1;
        } else {
            idx = static_cast<int>(std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin());
        }
        parents.emplace_back(cursor, idx);
        cursor = cursor->ptr[idx];
    }

    if (cursor->keys.empty() || key >= cursor->keys.back()) {
        cursor->keys.push_back(key);
        cursor->ptrs.push_back(val);
    } else {
        int insert_idx = static_cast<int>(std::lower_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin());
        cursor->keys.insert(cursor->keys.begin() + insert_idx, key);
        cursor->ptrs.insert(cursor->ptrs.begin() + insert_idx, val);
    }

    if (cursor->keys.size() <= (size_t)MAX) return;

    
    BPTreeNode* newLeaf = new BPTreeNode;
    newLeaf->IS_LEAF = true;
    int split = (MAX + 1) / 2;
    newLeaf->keys.assign(cursor->keys.begin() + split, cursor->keys.end());
    newLeaf->ptrs.assign(cursor->ptrs.begin() + split, cursor->ptrs.end());
    cursor->keys.resize(split);
    cursor->ptrs.resize(split);
    newLeaf->next = cursor->next;
    cursor->next = newLeaf;

    if (cursor == root) {
        BPTreeNode* newRoot = new BPTreeNode;
        newRoot->keys.push_back(newLeaf->keys[0]);
        newRoot->ptr.push_back(cursor);
        newRoot->ptr.push_back(newLeaf);
        root = newRoot;
        return;
    }

    propagateSplit(newLeaf->keys[0], newLeaf, parents);
}


// Thread-safe single-key insert with an exclusive tree lock.
void BPTree::insert(double key, size_t val) {
    std::unique_lock<std::shared_mutex> tree_lock(tree_latch);
    std::vector<std::pair<BPTreeNode*, int>> parents;
    parents.reserve(8);
    insertOne(key, val, parents);
}


// Inserts multiple entries without locking; caller must hold exclusive access.
void BPTree::bulkInsertUnlocked(const std::vector<std::pair<double, size_t>>& entries) {
    if (entries.empty()) return;
    
    bulkInsertImpl(entries);
}


// Thread-safe batch insert with an exclusive tree lock.
void BPTree::bulkInsert(const std::vector<std::pair<double, size_t>>& entries) {
    if (entries.empty()) return;
    std::unique_lock<std::shared_mutex> tree_lock(tree_latch);
    bulkInsertImpl(entries);
}


// Core batch insert logic with a fast rightmost-append path for sorted batches.
void BPTree::bulkInsertImpl(const std::vector<std::pair<double, size_t>>& entries) {

    std::vector<std::pair<BPTreeNode*, int>> parents;
    parents.reserve(8);

    
    
    
    if (entries.size() > 4 && root != nullptr) {
        BPTreeNode* rightmost = root;
        while (!rightmost->IS_LEAF)
            rightmost = rightmost->ptr.back();
        double cur_max = rightmost->keys.empty() ? -1e300 : rightmost->keys.back();
        if (!entries.empty() && entries.front().first > cur_max) {
            
            BPTreeNode* cursor = root;
            while (!cursor->IS_LEAF) {
                int idx = static_cast<int>(cursor->ptr.size()) - 1;
                parents.emplace_back(cursor, idx);
                cursor = cursor->ptr[idx];
            }
            
            for (const auto& [key, val] : entries) {
                cursor->keys.push_back(key);
                cursor->ptrs.push_back(val);

                if (cursor->keys.size() > (size_t)MAX) {
                    
                    BPTreeNode* newLeaf = new BPTreeNode;
                    newLeaf->IS_LEAF = true;
                    int split = (MAX + 1) / 2;
                    newLeaf->keys.assign(cursor->keys.begin() + split, cursor->keys.end());
                    newLeaf->ptrs.assign(cursor->ptrs.begin() + split, cursor->ptrs.end());
                    cursor->keys.resize(split);
                    cursor->ptrs.resize(split);
                    newLeaf->next = cursor->next;
                    cursor->next = newLeaf;

                    if (cursor == root) {
                        BPTreeNode* newRoot = new BPTreeNode;
                        newRoot->keys.push_back(newLeaf->keys[0]);
                        newRoot->ptr.push_back(cursor);
                        newRoot->ptr.push_back(newLeaf);
                        root = newRoot;
                        
                        parents.clear();
                        parents.emplace_back(root, 1);
                    } else {
                        propagateSplit(newLeaf->keys[0], newLeaf, parents);
                        
                        parents.clear();
                        BPTreeNode* c = root;
                        while (!c->IS_LEAF) {
                            int idx2 = static_cast<int>(c->ptr.size()) - 1;
                            parents.emplace_back(c, idx2);
                            c = c->ptr[idx2];
                        }
                        
                    }
                    cursor = newLeaf;
                }
            }
            return;
        }
    }

    
    for (const auto& e : entries) {
        insertOne(e.first, e.second, parents);
    }
}


// Returns all row IDs that exactly match the given key.
std::vector<size_t> BPTree::search(double key) {
    std::vector<size_t> results;
    std::shared_lock<std::shared_mutex> tree_lock(tree_latch);
    if (root == nullptr) return results;
    BPTreeNode* cursor = root;
    std::shared_lock<std::shared_mutex> cursor_lock(cursor->latch);
    while (!cursor->IS_LEAF) {
        int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), key) - cursor->keys.begin();
        BPTreeNode* child = cursor->ptr[idx];
        std::shared_lock<std::shared_mutex> child_lock(child->latch);
        cursor_lock.unlock();
        cursor = child;
        cursor_lock = std::move(child_lock);
    }
    for (size_t i = 0; i < cursor->keys.size(); i++) {
        if (cursor->keys[i] == key) results.push_back(cursor->ptrs[i]);
    }
    return results;
}


// Returns all row IDs whose key falls within the specified range.
std::vector<size_t> BPTree::searchRange(double start_key, double end_key, bool include_start, bool include_end) {
    std::vector<size_t> results;
    std::shared_lock<std::shared_mutex> tree_lock(tree_latch);
    if (root == nullptr) return results;
    BPTreeNode* cursor = root;
    std::shared_lock<std::shared_mutex> cursor_lock(cursor->latch);
    while (!cursor->IS_LEAF) {
        int idx = std::upper_bound(cursor->keys.begin(), cursor->keys.end(), start_key) - cursor->keys.begin();
        BPTreeNode* child = cursor->ptr[idx];
        std::shared_lock<std::shared_mutex> child_lock(child->latch);
        cursor_lock.unlock();
        cursor = child;
        cursor_lock = std::move(child_lock);
    }
    while (cursor != nullptr) {
        for (size_t i = 0; i < cursor->keys.size(); i++) {
            bool after_start = include_start ? (cursor->keys[i] >= start_key) : (cursor->keys[i] > start_key);
            bool before_end = include_end ? (cursor->keys[i] <= end_key) : (cursor->keys[i] < end_key);
            if (after_start && before_end) {
                results.push_back(cursor->ptrs[i]);
            }
            if (!before_end) return results; 
        }
        BPTreeNode* next = cursor->next;
        if (!next) break;
        std::shared_lock<std::shared_mutex> next_lock(next->latch);
        cursor_lock.unlock();
        cursor = next;
        cursor_lock = std::move(next_lock);
    }
    return results;
}
