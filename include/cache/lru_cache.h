#ifndef LRU_CACHE_H
#define LRU_CACHE_H
#include <string>
#include <string_view>
#include <vector>
#include <array>
#include <list>
#include <unordered_map>
#include <shared_mutex>
#include <fstream>
#include <cstdio>
#include <cmath>
#include <memory>
#include "index/bptree.h"

// In-memory storage block using a PAX (partition attributes across) layout.
struct PaxBlock {
    int num_rows = 0;                        // Total number of rows stored in this block.
    std::vector<uint8_t>  tombstones;        // Per-row deletion flag; 1 = deleted.
    std::vector<uint64_t> expirations;       // Per-row expiry timestamp (0 = no expiry).

    std::vector<uint8_t> col_is_numeric;     // 1 if column stores doubles, 0 if text.
    std::vector<int>  col_num_idx;           // Maps column index to its slot in num_cols.
    std::vector<int>  col_str_idx;           // Maps column index to its slot in str_cols.

    std::vector<std::vector<double>>      num_cols;  // Storage for numeric columns.
    std::vector<std::vector<std::string>> str_cols;  // Storage for text columns.

    std::vector<std::vector<std::string>> columns;   // Legacy flat storage (unused in typed path).

    // Converts an integer to string faster than snprintf for the common case.
    static std::string fast_lltoa(long long v) {
        
        char buf[22];
        char* p = buf + 21;
        *p = '\0';
        bool neg = v < 0;
        unsigned long long u = neg ? static_cast<unsigned long long>(-(v + 1)) + 1ULL
                                   : static_cast<unsigned long long>(v);
        if (u == 0) { *--p = '0'; }
        else { while (u) { *--p = '0' + static_cast<char>(u % 10); u /= 10; } }
        if (neg) *--p = '-';
        return std::string(p, static_cast<size_t>(buf + 21 - p));
    }

    // Returns the string value of a cell, choosing numeric or text storage as needed.
    std::string get_str(size_t col, size_t row) const {
        if (col_is_numeric.empty()) return columns[col][row];
        if (col_is_numeric[col]) {
            double v = num_cols[col_num_idx[col]][row];
            
            if (v >= -9.007199254740992e15 && v <= 9.007199254740992e15) {
                long long iv = static_cast<long long>(v);
                if (static_cast<double>(iv) == v) {
                    return fast_lltoa(iv);
                }
            }
            char tmp[64];
            int n = std::snprintf(tmp, sizeof(tmp), "%.15g", v);
            return {tmp, static_cast<size_t>(n)};
        }
        return str_cols[col_str_idx[col]][row];
    }

    // Clears all row data while keeping the column structure intact.
    void clear_data() {
        num_rows = 0;
        tombstones.clear();
        expirations.clear();
        for (auto& v : num_cols)  v.clear();
        for (auto& v : str_cols)  v.clear();
        for (auto& v : columns)   v.clear();
    }
};

// Per-table in-memory cache holding schema, data block, index, and file handles.
struct TableCache {
    std::string db_name;                    // Name of the database this table belongs to.
    std::string table_name;                 // Name of this table.
    std::vector<std::string> columns;       // Ordered list of column names.
    std::vector<int> col_types;             // Column types: 0=INT, 1=DOUBLE, 2=TEXT.
    int index_col_idx  = 0;                 // Index of the primary key column.
    std::string index_col_name;                    // Name of the primary key column.
    std::vector<std::pair<double, size_t>> pending_index_entries; // Buffered index entries not yet inserted.
    std::array<std::shared_mutex, 4096> page_locks; // Fine-grained per-page read-write locks.
    bool page_cache_dirty   = false;               // True if the page cache has unsaved data.
    bool binary_files_ready = false;               // True once binary data and index files exist.
    double max_indexed_key  = -1e300;              // Tracks the largest indexed key for fast dedup.

    std::mutex append_stream_mutex;                // Protects concurrent access to append file descriptors.

    int data_append_fd  = -1;                      // File descriptor for appending to data.dat.
    int index_append_fd = -1;                      // File descriptor for appending to index.dat.
    int wal_fd          = -1;                      // File descriptor for the WAL log file.
    uint32_t wal_batch_count = 0;                  // Number of WAL batches written since last checkpoint.

    static constexpr size_t kStreamBuf = 64 * 1024 * 1024; // Stream buffer size (64 MB).
    std::unique_ptr<char[]> data_stream_buf;       // Buffer backing the data append stream.
    std::unique_ptr<char[]> idx_stream_buf;        // Buffer backing the index append stream.
    std::ofstream data_append_stream;              // Legacy ofstream for data file (kept for compatibility).
    std::ofstream index_append_stream;             // Legacy ofstream for index file (kept for compatibility).

    size_t page_cache_capacity = 2048;             // Maximum number of pages held in the LRU cache.
    std::unordered_map<uint32_t, std::vector<int>> page_cache_rows; // Maps page ID to its live row indices.
    std::list<uint32_t> page_cache_lru;            // LRU ordering of cached page IDs.
    std::unordered_map<uint32_t, std::list<uint32_t>::iterator> page_cache_pos; // Iterator into LRU list per page.
    std::unordered_map<uint32_t, uint32_t> page_scan_touch; // Scan-resistance touch counter per page.
    std::mutex page_cache_mutex;                   // Protects the page cache data structures.

    BPTree primary_index;    // B+ tree index on the primary key column.
    PaxBlock mem_block;      // In-memory PAX storage block for all table rows.
    std::shared_mutex rw_mutex; // Read-write lock protecting the in-memory block.

    // Initializes the table cache with the given database and table name.
    TableCache(std::string db, std::string name);
    // Destructor for TableCache.
    ~TableCache();
    // No-op stub; persistence is handled by the server write path.
    void flush_mem_block();
};
#endif
