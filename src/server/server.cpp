#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include <string_view>
#include <sys/uio.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <shared_mutex>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cache/lru_cache.h"

using namespace std;
namespace fs = std::filesystem;

static const string kDataDir = "data";
static const uint32_t kDataMagicV1 = 0x314C5146;  
static const uint32_t kDataMagicV2 = 0x32505146;  
static const uint32_t kIndexMagic = 0x31585146;  
static const uint32_t kWalMagic  = 0x314C4157;  
static const uint32_t kRowsPerPage = 1024;
static const uint32_t kWalCheckpointInterval = 200;  


unordered_map<string, unordered_map<string, TableCache*>> databases;
mutex db_mutex;

queue<int> g_client_queue;
mutex g_client_queue_mutex;
condition_variable g_client_queue_cv;


// Returns the filesystem path to a database directory.
static fs::path db_root(const string& db_name);

// Returns the filesystem path to a table's data subdirectory.
static fs::path table_path(const string& db_name, const string& table_name);

// Returns the path to a table's binary data file.
static fs::path data_path(const string& db_name, const string& table_name);

// Returns the path to a table's B+ tree index file.
static fs::path index_path(const string& db_name, const string& table_name);

// Returns the path to a table's WAL log file.
static fs::path wal_path(const string& db_name, const string& table_name);


// Packs a page ID and slot number into a single 64-bit row ID.
static inline size_t encode_rid(uint32_t page_id, uint32_t slot) {
    return (static_cast<size_t>(page_id) << 32) | static_cast<size_t>(slot);
}


// Extracts the page ID from a packed row ID.
static inline uint32_t rid_page(size_t rid) {
    return static_cast<uint32_t>(rid >> 32);
}


// Extracts the slot number from a packed row ID.
static inline uint32_t rid_slot(size_t rid) {
    return static_cast<uint32_t>(rid & 0xFFFFFFFFu);
}


// Converts a flat row index to a page-slot row ID.
static inline size_t row_to_rid(size_t row_idx) {
    
    return encode_rid(static_cast<uint32_t>(row_idx / kRowsPerPage), static_cast<uint32_t>(row_idx % kRowsPerPage));
}


// Converts a page-slot row ID back to a flat row index.
static inline size_t rid_to_row(size_t rid) {
    
    return static_cast<size_t>(rid_page(rid)) * kRowsPerPage + static_cast<size_t>(rid_slot(rid));
}


// Writes a uint8 value to a binary output stream.
static void write_u8(ostream& out, uint8_t v) { out.write(reinterpret_cast<const char*>(&v), sizeof(v)); }

// Writes a uint32 value to a binary output stream.
static void write_u32(ostream& out, uint32_t v) { out.write(reinterpret_cast<const char*>(&v), sizeof(v)); }

// Writes a uint64 value to a binary output stream.
static void write_u64(ostream& out, uint64_t v) { out.write(reinterpret_cast<const char*>(&v), sizeof(v)); }

// Writes a double value to a binary output stream.
static void write_f64(ostream& out, double v) { out.write(reinterpret_cast<const char*>(&v), sizeof(v)); }


// Appends raw bytes to a string buffer.
static inline void append_bytes(string& out, const void* data, size_t len) {
    out.append(reinterpret_cast<const char*>(data), len);
}


// Appends a uint8 value to a string buffer.
static inline void append_u8(string& out, uint8_t v) { append_bytes(out, &v, sizeof(v)); }

// Appends a uint32 value to a string buffer.
static inline void append_u32(string& out, uint32_t v) { append_bytes(out, &v, sizeof(v)); }

// Appends a uint64 value to a string buffer.
static inline void append_u64(string& out, uint64_t v) { append_bytes(out, &v, sizeof(v)); }

// Appends a double value to a string buffer.
static inline void append_f64(string& out, double v) { append_bytes(out, &v, sizeof(v)); }


// Overwrites a uint32 at a given byte position in a string buffer.
static inline void patch_u32(string& out, size_t pos, uint32_t v) {
    if (pos + sizeof(uint32_t) <= out.size()) {
        memcpy(&out[pos], &v, sizeof(v));
    }
}


// Reads a uint8 value from a binary input stream.
static bool read_u8(istream& in, uint8_t& v) { return static_cast<bool>(in.read(reinterpret_cast<char*>(&v), sizeof(v))); }

// Reads a uint32 value from a binary input stream.
static bool read_u32(istream& in, uint32_t& v) { return static_cast<bool>(in.read(reinterpret_cast<char*>(&v), sizeof(v))); }

// Reads a uint64 value from a binary input stream.
static bool read_u64(istream& in, uint64_t& v) { return static_cast<bool>(in.read(reinterpret_cast<char*>(&v), sizeof(v))); }


// Creates or validates the magic-number header of a binary file.
static bool ensure_binary_file_header(const fs::path& p, uint32_t magic) {
    if (!fs::exists(p) || fs::file_size(p) < sizeof(uint32_t)) {
        ofstream out(p, ios::binary | ios::trunc);
        if (!out) return false;
        
        write_u32(out, magic);
        return static_cast<bool>(out);
    }
    ifstream in(p, ios::binary);
    if (!in) return false;
    uint32_t existing = 0;
    
    if (!read_u32(in, existing)) return false;
    if (existing == magic) return true;
    return false;
}


// Returns the mutex array index for a given page ID.
static inline size_t page_lock_idx(uint32_t page_id) {
    return static_cast<size_t>(page_id) & 4095u;
}


// Opens the data, index, and WAL file descriptors if not already open.
static bool ensure_append_streams_open(TableCache* t) {
    if (t->data_append_fd < 0) {
        
        fs::create_directories(table_path(t->db_name, t->table_name));
        
        t->data_append_fd = open(data_path(t->db_name, t->table_name).c_str(),
                                 O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (t->data_append_fd < 0) return false;
    }
    if (t->index_append_fd < 0) {
        
        fs::create_directories(index_path(t->db_name, t->table_name).parent_path());
        
        t->index_append_fd = open(index_path(t->db_name, t->table_name).c_str(),
                                  O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (t->index_append_fd < 0) return false;
    }
    if (t->wal_fd < 0) {
        
        t->wal_fd = open(wal_path(t->db_name, t->table_name).c_str(),
                         O_RDWR | O_CREAT | O_APPEND, 0644);
        if (t->wal_fd >= 0) {
            struct stat wst;
            if (fstat(t->wal_fd, &wst) == 0 && wst.st_size == 0) {
                uint32_t magic = kWalMagic;
                ::write(t->wal_fd, &magic, sizeof(magic));
            }
        }
    }
    return true;
}


// Flushes and closes the data, index, and WAL file descriptors.
static void close_append_streams(TableCache* t) {
    if (t->data_append_fd >= 0)  { ::close(t->data_append_fd);  t->data_append_fd  = -1; }
    if (t->index_append_fd >= 0) { ::close(t->index_append_fd); t->index_append_fd = -1; }
    if (t->wal_fd >= 0)          { ::close(t->wal_fd);          t->wal_fd          = -1; }
    if (t->data_append_stream.is_open())  { t->data_append_stream.flush();  t->data_append_stream.close(); }
    if (t->index_append_stream.is_open()) { t->index_append_stream.flush(); t->index_append_stream.close(); }
}


// Moves a page to the front of the LRU list (marks it recently used).
static void page_cache_touch(TableCache* t, uint32_t page_id) {
    auto it = t->page_cache_pos.find(page_id);
    if (it != t->page_cache_pos.end()) {
        t->page_cache_lru.splice(t->page_cache_lru.begin(), t->page_cache_lru, it->second);
        it->second = t->page_cache_lru.begin();
    }
}


// Evicts the least-recently-used pages when cache exceeds capacity.
static void page_cache_evict_if_needed(TableCache* t) {
    while (t->page_cache_rows.size() > t->page_cache_capacity && !t->page_cache_lru.empty()) {
        uint32_t victim = t->page_cache_lru.back();
        t->page_cache_lru.pop_back();
        t->page_cache_pos.erase(victim);
        t->page_cache_rows.erase(victim);
    }
}


// Removes all entries from the page LRU cache.
static void page_cache_clear(TableCache* t) {
    lock_guard<mutex> cache_lock(t->page_cache_mutex);
    t->page_cache_rows.clear();
    t->page_cache_lru.clear();
    t->page_cache_pos.clear();
    t->page_scan_touch.clear();
    t->page_cache_dirty = false;
}


// Returns live row indices for a page, loading from memory if not cached.
static const vector<int>& page_cache_get_rows(TableCache* t, uint32_t page_id, bool sequential_scan_hint) {
    lock_guard<mutex> cache_lock(t->page_cache_mutex);
    auto hit = t->page_cache_rows.find(page_id);
    if (hit != t->page_cache_rows.end()) {
        
        page_cache_touch(t, page_id);
        return hit->second;
    }

    bool admit = true;
    if (sequential_scan_hint) {
        uint32_t& touches = t->page_scan_touch[page_id];
        touches++;
        
        admit = (touches >= 2);
    }

    static const vector<int> empty_rows;
    if (!admit) {
        return empty_rows;
    }

    size_t start = static_cast<size_t>(page_id) * kRowsPerPage;
    size_t end = min(static_cast<size_t>(t->mem_block.num_rows), start + kRowsPerPage);

    vector<int> rows;
    rows.reserve(end > start ? (end - start) : 0);
    for (size_t i = start; i < end; ++i) {
        if (t->mem_block.tombstones[i] == 0) {
            rows.push_back(static_cast<int>(i));
        }
    }

    t->page_cache_lru.push_front(page_id);
    t->page_cache_pos[page_id] = t->page_cache_lru.begin();
    auto inserted = t->page_cache_rows.emplace(page_id, std::move(rows));
    
    page_cache_evict_if_needed(t);
    return inserted.first->second;
}


// Sends a length-prefixed message string to the client socket.
static void send_response(int client_socket, const string& msg) {
    uint32_t len = htonl(static_cast<uint32_t>(msg.length()));
    struct iovec iov[2];
    iov[0].iov_base = &len;
    iov[0].iov_len = sizeof(len);
    iov[1].iov_base = (void*)msg.data();
    iov[1].iov_len = msg.length();
    if (writev(client_socket, iov, msg.empty() ? 1 : 2)) {}
}


// Sends an ERROR-prefixed message to the client socket.
static void send_error(int client_socket, const string& msg) {
    
    send_response(client_socket, "ERROR:" + msg);
}


// Returns a copy of a string with leading and trailing whitespace removed.
static string trim_copy(string s) {
    size_t start = 0;
    while (start < s.size() && isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}


// Returns a trimmed string_view without any heap allocation.
static string_view trim_view(string_view s) {
    size_t start = 0;
    while (start < s.size() && isspace(static_cast<unsigned char>(s[start]))) start++;
    size_t end = s.size();
    while (end > start && isspace(static_cast<unsigned char>(s[end - 1]))) end--;
    return s.substr(start, end - start);
}


// Converts a single ASCII character to its uppercase equivalent.
static inline char upper_ascii(char c) {
    return static_cast<char>(toupper(static_cast<unsigned char>(c)));
}


// Returns true if a string starts with a prefix, case-insensitive.
static bool starts_with_ci_view(string_view s, string_view prefix) {
    if (s.size() < prefix.size()) return false;
    for (size_t i = 0; i < prefix.size(); ++i) {
        
        if (upper_ascii(s[i]) != upper_ascii(prefix[i])) return false;
    }
    return true;
}


// Finds the first case-insensitive occurrence of a substring.
static size_t find_ci_view(string_view haystack, string_view needle, size_t start = 0) {
    if (needle.empty()) return start <= haystack.size() ? start : string::npos;
    if (haystack.size() < needle.size() || start >= haystack.size()) return string::npos;
    for (size_t i = start; i + needle.size() <= haystack.size(); ++i) {
        size_t j = 0;
        
        while (j < needle.size() && upper_ascii(haystack[i + j]) == upper_ascii(needle[j])) {
            j++;
        }
        if (j == needle.size()) return i;
    }
    return string::npos;
}


// Returns an uppercase copy of the given string.
static string upper_copy(string s) {
    transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(toupper(c)); });
    return s;
}


// Returns the current Unix timestamp in seconds.
static uint64_t current_time_sec() {
    return chrono::duration_cast<chrono::seconds>(chrono::system_clock::now().time_since_epoch()).count();
}


// Parses a double from a std::string, using a fast integer path.
static bool parse_double_fast(const string& s, double& out) {
    if (s.empty()) return false;
    size_t i = 0;
    bool neg = false;
    if (s[i] == '+' || s[i] == '-') { neg = (s[i] == '-'); i++; if (i >= s.size()) return false; }
    bool all_digits = true;
    uint64_t v = 0;
    for (size_t j = i; j < s.size(); ++j) {
        char c = s[j];
        if (c < '0' || c > '9') { all_digits = false; break; }
        v = (v * 10u) + static_cast<uint64_t>(c - '0');
    }
    if (all_digits) { out = neg ? -static_cast<double>(v) : static_cast<double>(v); return true; }
    const char* begin = s.data();
    char* end = nullptr;
    errno = 0;
    out = strtod(begin, &end);
    return end == (begin + s.size()) && errno != ERANGE;
}


// Parses a double from a string_view with no heap allocation.
static bool parse_double_fast_sv(string_view s, double& out) {
    if (s.empty()) return false;
    size_t i = 0;
    bool neg = false;
    if (s[i] == '+' || s[i] == '-') { neg = (s[i] == '-'); i++; if (i >= s.size()) return false; }
    bool all_digits = true;
    uint64_t v = 0;
    for (size_t j = i; j < s.size(); ++j) {
        char c = s[j];
        if (c < '0' || c > '9') { all_digits = false; break; }
        v = (v * 10u) + static_cast<uint64_t>(c - '0');
    }
    if (all_digits) { out = neg ? -static_cast<double>(v) : static_cast<double>(v); return true; }
    
    const char* begin = s.data();
    char* end2 = nullptr;
    errno = 0;
    out = strtod(begin, &end2);
    return end2 == (begin + s.size()) && errno != ERANGE;
}


// Trims whitespace and strips outer quotes from a string_view token.
static inline string_view parse_token_sv(string_view tok) {
    size_t a = 0, b = tok.size();
    while (a < b && isspace(static_cast<unsigned char>(tok[a]))) a++;
    while (b > a && isspace(static_cast<unsigned char>(tok[b-1]))) b--;
    tok = tok.substr(a, b - a);
    if (tok.size() >= 2 &&
        ((tok.front() == '\'' && tok.back() == '\'') ||
         (tok.front() == '"'  && tok.back() == '"'))) {
        tok = tok.substr(1, tok.size() - 2);
    }
    return tok;
}


template <typename Fn>

// Iterates over each parenthesized value-group in an INSERT VALUES clause.
static bool for_each_insert_group_sv(string_view vp, size_t hint_ncols, Fn&& fn) {
    
    vector<string_view> toks;
    toks.reserve(hint_ncols > 0 ? hint_ncols : 8);

    
    static const auto ct = []() {
        std::array<uint8_t, 256> t{};
        t[static_cast<uint8_t>('\'')] = 1;
        t[static_cast<uint8_t>('"')]  = 1;
        t[static_cast<uint8_t>('(')]  = 2;
        t[static_cast<uint8_t>(')')]  = 4;
        t[static_cast<uint8_t>(',')]  = 8;
        return t;
    }();

    const char* const base = vp.data();
    size_t i = 0, n = vp.size();

    while (i < n) {
        
        while (i < n && vp[i] != '(') i++;
        if (i >= n) break;
        i++;  

        toks.clear();
        bool in_quote = false;
        char qch = 0;
        int nested = 0;
        size_t ts = i;

        while (i < n) {
            if (in_quote) {
                
                const char* q = static_cast<const char*>(memchr(base + i, qch, n - i));
                if (!q) { i = n; break; }
                i = static_cast<size_t>(q - base) + 1;
                in_quote = false; qch = 0;
                continue;
            }

            
            while (i < n && !ct[static_cast<uint8_t>(vp[i])]) i++;
            if (i >= n) break;

            char ch = vp[i];
            uint8_t ctype = ct[static_cast<uint8_t>(ch)];

            if (ctype == 1) {  
                in_quote = true; qch = ch; i++; continue;
            }
            if (ctype == 2) {  
                nested++; i++; continue;
            }
            if (ctype == 4) {  
                if (nested == 0) {
                    
                    toks.push_back(parse_token_sv(vp.substr(ts, i - ts)));
                    i++; break;
                }
                nested--; i++; continue;
            }
            
            if (nested == 0) {
                
                toks.push_back(parse_token_sv(vp.substr(ts, i - ts)));
                ts = i + 1;
            }
            i++;
        }
        if (!toks.empty()) {
            if (!fn(toks.data(), toks.size())) return false;
        }
    }
    return true;
}


// Removes surrounding single or double quotes from a string.
static string dequote(string s) {
    
    s = trim_copy(s);
    if (s.size() >= 2 && ((s.front() == '\'' && s.back() == '\'') || (s.front() == '"' && s.back() == '"'))) {
        s = s.substr(1, s.size() - 2);
    }
    return s;
}


// Strips the table-name prefix from a qualified column name.
static string unqualify_col(string c) {
    
    c = trim_copy(c);
    auto dot = c.find('.');
    if (dot != string::npos) return c.substr(dot + 1);
    return c;
}


// Splits a comma-separated string, respecting quotes and nested parentheses.
static vector<string> split_csv_top_level(const string& s) {
    vector<string> out;
    string cur;
    bool in_quote = false;
    char quote_char = 0;
    int paren_depth = 0;
    for (char ch : s) {
        if ((ch == '\'' || ch == '"') && (quote_char == 0 || ch == quote_char)) {
            if (!in_quote) {
                in_quote = true;
                quote_char = ch;
            } else {
                in_quote = false;
                quote_char = 0;
            }
            cur.push_back(ch);
            continue;
        }
        if (!in_quote) {
            if (ch == '(') paren_depth++;
            if (ch == ')') paren_depth--;
            if (ch == ',' && paren_depth == 0) {
                
                out.push_back(trim_copy(cur));
                cur.clear();
                continue;
            }
        }
        cur.push_back(ch);
    }
    
    if (!cur.empty()) out.push_back(trim_copy(cur));
    return out;
}


// Expands backslash escape sequences in a field string.
static string unescape_field(const string& s) {
    string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        if (s[i] == '\\' && i + 1 < s.size()) {
            char n = s[i + 1];
            if (n == 't') {
                out.push_back('\t');
                i++;
                continue;
            }
            if (n == 'n') {
                out.push_back('\n');
                i++;
                continue;
            }
            if (n == '\\') {
                out.push_back('\\');
                i++;
                continue;
            }
        }
        out.push_back(s[i]);
    }
    return out;
}


// Splits a tab-delimited line into a vector of field strings.
static vector<string> split_tab_escaped(const string& line) {
    vector<string> out;
    string cur;
    for (size_t i = 0; i < line.size(); ++i) {
        if (line[i] == '\t') {
            out.push_back(cur);
            cur.clear();
            continue;
        }
        cur.push_back(line[i]);
    }
    out.push_back(cur);
    return out;
}


// Returns the filesystem path to a database directory.
static fs::path db_root(const string& db_name) {
    return fs::path(kDataDir) / db_name;
}


// Returns the filesystem path to a table's data subdirectory.
static fs::path table_path(const string& db_name, const string& table_name) {
    
    return db_root(db_name) / "tables" / table_name;
}


// Returns the filesystem path to a table's schema file.
static fs::path schema_path(const string& db_name, const string& table_name) {
    
    return db_root(db_name) / "schema" / (table_name + ".sch");
}


// Returns the path to a table's binary data file.
static fs::path data_path(const string& db_name, const string& table_name) {
    
    return db_root(db_name) / "tables" / table_name / "data.dat";
}


// Returns the path to a table's B+ tree index file.
static fs::path index_path(const string& db_name, const string& table_name) {
    
    return db_root(db_name) / "index" / table_name / "index.dat";
}


// Returns the path to a table's WAL log file.
static fs::path wal_path(const string& db_name, const string& table_name) {
    
    return db_root(db_name) / "tables" / table_name / "wal.log";
}


// Finds the column index by name (or qualified name) in a table.
static int resolve_col_idx(TableCache* t, const string& maybe_qualified) {
    
    string col = unqualify_col(maybe_qualified);
    for (int i = 0; i < static_cast<int>(t->columns.size()); ++i) {
        if (t->columns[i] == col) return i;
    }
    return -1;
}


// Initializes PAX typed column arrays from the table's column type metadata.
static void setup_pax_types(TableCache* t) {
    size_t ncols = t->columns.size();
    auto& mb = t->mem_block;
    mb.col_is_numeric.resize(ncols);
    mb.col_num_idx.assign(ncols, -1);
    mb.col_str_idx.assign(ncols, -1);
    mb.num_cols.clear();
    mb.str_cols.clear();
    mb.columns.resize(0);  
    for (size_t c = 0; c < ncols; ++c) {
        int ct = t->col_types[c];  
        if (ct == 0 || ct == 1) {
            mb.col_is_numeric[c] = true;
            mb.col_num_idx[c] = static_cast<int>(mb.num_cols.size());
            mb.num_cols.emplace_back();
        } else {
            mb.col_is_numeric[c] = false;
            mb.col_str_idx[c] = static_cast<int>(mb.str_cols.size());
            mb.str_cols.emplace_back();
        }
    }
}


// Returns the string value of a cell from the in-memory PAX block.
static inline string cell_str(const TableCache* t, int col, int row) {
    return t->mem_block.get_str(static_cast<size_t>(col), static_cast<size_t>(row));
}


// Flushes any pending index entries into the B+ tree.
static void materialize_pending_index(TableCache* t) {
    
    if (t->pending_index_entries.empty()) return;
    for (const auto& kv : t->pending_index_entries) {
        t->primary_index.insert(kv.first, kv.second);
    }
    t->pending_index_entries.clear();
    
}


// Writes the table schema (column names and types) to a .sch file on disk.
static bool persist_schema(TableCache* t) {
    
    fs::create_directories(db_root(t->db_name) / "schema");
    
    ofstream out(schema_path(t->db_name, t->table_name));
    if (!out) return false;
    out << "INDEX_COL:" << t->index_col_name << "\n";
    for (size_t i = 0; i < t->columns.size(); ++i) {
        out << t->columns[i] << ":" << t->col_types[i] << "\n";
    }
    return true;
}


// Serializes a single row (tombstone, expiry, values) to a binary stream.
static void append_row_to_stream(ofstream& out, const vector<string>& row, uint64_t expires_at, uint8_t tombstone) {
    
    write_u8(out, tombstone);
    
    write_u64(out, expires_at);
    
    write_u32(out, static_cast<uint32_t>(row.size()));
    for (const auto& v : row) {
        
        write_u32(out, static_cast<uint32_t>(v.size()));
        if (!v.empty()) out.write(v.data(), static_cast<streamsize>(v.size()));
    }
}


// Rewrites data.dat and index.dat from scratch using the in-memory block.
static bool rewrite_table_binary_files(TableCache* t) {
    
    ofstream data_out(data_path(t->db_name, t->table_name), ios::binary | ios::trunc);
    
    ofstream idx_out(index_path(t->db_name, t->table_name), ios::binary | ios::trunc);
    if (!data_out || !idx_out) return false;

    
    write_u32(data_out, kDataMagicV2);
    
    write_u32(idx_out, kIndexMagic);

    for (int base = 0; base < t->mem_block.num_rows; base += static_cast<int>(kRowsPerPage)) {
        uint32_t page_id = static_cast<uint32_t>(base / static_cast<int>(kRowsPerPage));
        uint32_t row_count = static_cast<uint32_t>(min(t->mem_block.num_rows - base, static_cast<int>(kRowsPerPage)));
        
        write_u32(data_out, page_id);
        
        write_u32(data_out, row_count);

        for (uint32_t slot = 0; slot < row_count; ++slot) {
            int i = base + static_cast<int>(slot);
            vector<string> row;
            row.reserve(t->columns.size());
            
            for (size_t c = 0; c < t->columns.size(); ++c) row.push_back(cell_str(t, static_cast<int>(c), i));
            
            append_row_to_stream(data_out, row, t->mem_block.expirations[i], t->mem_block.tombstones[i]);
        }

        for (uint32_t slot = 0; slot < row_count; ++slot) {
            int i = base + static_cast<int>(slot);
            if (t->mem_block.tombstones[i] == 0) {
                double key = 0.0;
                
                string ks = cell_str(t, t->index_col_idx, i);
                
                if (parse_double_fast(ks, key)) {
                    
                    size_t rid = row_to_rid(static_cast<size_t>(i));
                    
                    write_f64(idx_out, key);
                    
                    write_u32(idx_out, rid_page(rid));
                    
                    write_u32(idx_out, rid_slot(rid));
                }
            }
        }
    }

    return static_cast<bool>(data_out) && static_cast<bool>(idx_out);
}


// Appends a WAL entry (header + payload) to the open WAL file descriptor.
static void write_wal_entry(int wal_fd, uint32_t start_row, uint32_t num_rows,
                            const char* payload, size_t payload_len) {
    uint32_t entry_size = static_cast<uint32_t>(12 + payload_len);
    uint32_t hdr[3] = {entry_size, start_row, num_rows};
    struct iovec iov[2];
    iov[0].iov_base = hdr;
    iov[0].iov_len  = 12;
    iov[1].iov_base = const_cast<char*>(payload);
    iov[1].iov_len  = payload_len;
    ::writev(wal_fd, iov, 2);
}


// Resets the WAL file to contain only the magic header.
static void truncate_wal(int wal_fd) {
    if (wal_fd < 0) return;
    if (ftruncate(wal_fd, 0) == 0) {
        lseek(wal_fd, 0, SEEK_SET);
        uint32_t magic = kWalMagic;
        ::write(wal_fd, &magic, sizeof(magic));
    }
}


// Replays WAL entries not in data.dat after a crash, then truncates the WAL.
static void replay_wal(const string& db_name, const string& table_name,
                       TableCache* t,
                       vector<pair<double, size_t>>& bulk_index_entries) {
    
    auto wp = wal_path(db_name, table_name);
    if (!fs::exists(wp)) return;
    auto fsize = fs::file_size(wp);
    if (fsize <= 4) return;

    ifstream wf(wp, ios::binary);
    if (!wf) return;

    uint32_t magic = 0;
    
    if (!read_u32(wf, magic) || magic != kWalMagic) return;

    vector<char> buf(fsize - 4);
    wf.read(buf.data(), static_cast<streamsize>(buf.size()));
    wf.close();

    int data_dat_rows = t->mem_block.num_rows;
    bool replayed = false;

    
    
    int data_fd = open(data_path(db_name, table_name).c_str(),
                       O_WRONLY | O_CREAT | O_APPEND, 0644);

    bool pax = !t->mem_block.col_is_numeric.empty();
    size_t ncols = t->columns.size();

    size_t pos = 0;
    while (pos + 12 <= buf.size()) {
        uint32_t entry_size, start_row, num_rows;
        memcpy(&entry_size, buf.data() + pos, 4);
        memcpy(&start_row, buf.data() + pos + 4, 4);
        memcpy(&num_rows, buf.data() + pos + 8, 4);

        if (entry_size < 12 || pos + entry_size > buf.size()) break;

        size_t payload_len = entry_size - 12;
        const char* payload = buf.data() + pos + 12;

        if (static_cast<int>(start_row) >= data_dat_rows && payload_len > 0) {
            
            size_t pp = 0;
            while (pp + 8 <= payload_len) {
                uint32_t page_id, row_count;
                memcpy(&page_id, payload + pp, 4); pp += 4;
                memcpy(&row_count, payload + pp, 4); pp += 4;

                for (uint32_t r = 0; r < row_count && pp + 13 <= payload_len; r++) {
                    uint8_t tomb;
                    uint64_t exp;
                    uint32_t nc;
                    memcpy(&tomb, payload + pp, 1); pp += 1;
                    memcpy(&exp,  payload + pp, 8); pp += 8;
                    memcpy(&nc,   payload + pp, 4); pp += 4;

                    if (nc != ncols) goto done_replay;

                    int row_idx = t->mem_block.num_rows;
                    t->mem_block.num_rows++;
                    t->mem_block.tombstones.push_back(tomb);
                    t->mem_block.expirations.push_back(exp);

                    double key = 0;
                    for (uint32_t c = 0; c < nc && pp + 4 <= payload_len; c++) {
                        uint32_t cl;
                        memcpy(&cl, payload + pp, 4); pp += 4;
                        string val;
                        if (cl > 0 && pp + cl <= payload_len) {
                            val.assign(payload + pp, cl);
                            pp += cl;
                        }
                        if (pax) {
                            if (t->mem_block.col_is_numeric[c]) {
                                double v = 0;
                                
                                parse_double_fast(val, v);
                                t->mem_block.num_cols[t->mem_block.col_num_idx[c]].push_back(v);
                                if (static_cast<int>(c) == t->index_col_idx) key = v;
                            } else {
                                t->mem_block.str_cols[t->mem_block.col_str_idx[c]].push_back(std::move(val));
                            }
                        } else {
                            if (static_cast<int>(c) == t->index_col_idx)
                                
                                parse_double_fast(val, key);
                            t->mem_block.columns[c].push_back(std::move(val));
                        }
                    }
                    if (tomb == 0) {
                        
                        bulk_index_entries.emplace_back(key, row_to_rid(static_cast<size_t>(row_idx)));
                    }
                }
            }
            
            if (data_fd >= 0) {
                size_t w = 0;
                while (w < payload_len) {
                    ssize_t n = ::write(data_fd, payload + w, payload_len - w);
                    if (n <= 0) break;
                    w += static_cast<size_t>(n);
                }
            }
            replayed = true;
        }
        pos += entry_size;
    }
done_replay:
    if (data_fd >= 0) ::close(data_fd);

    
    if (replayed || pos > 0) {
        int wfd = open(wp.c_str(), O_RDWR | O_TRUNC, 0644);
        if (wfd >= 0) {
            uint32_t m = kWalMagic;
            ::write(wfd, &m, sizeof(m));
            ::close(wfd);
        }
    }
}


// Loads a table's schema, binary data, and index from disk into memory.
static bool load_table_from_disk(const string& db_name, const string& table_name) {
    
    ifstream schema_in(schema_path(db_name, table_name));
    if (!schema_in) return false;

    auto* t = new TableCache(db_name, table_name);

    string line;
    while (getline(schema_in, line)) {
        
        line = trim_copy(line);
        if (line.empty()) continue;
        auto colon = line.find(':');
        if (colon == string::npos) continue;
        string key = line.substr(0, colon);
        string val = line.substr(colon + 1);

        if (key == "INDEX_COL") {
            
            t->index_col_name = trim_copy(val);
            continue;
        }

        
        t->columns.push_back(trim_copy(key));
        
        t->col_types.push_back(atoi(trim_copy(val).c_str()));
    }

    if (t->columns.empty()) {
        delete t;
        return false;
    }

    if (t->index_col_name.empty()) t->index_col_name = t->columns[0];
    
    t->index_col_idx = resolve_col_idx(t, t->index_col_name);
    if (t->index_col_idx < 0) t->index_col_idx = 0;

    
    
    setup_pax_types(t);
    t->pending_index_entries.reserve(1 << 20);
    
    page_cache_clear(t);
    vector<pair<double, size_t>> bulk_index_entries;

    
    auto push_field = [&](size_t c, string& v) {
        if (t->mem_block.col_is_numeric[c]) {
            double dv = 0.0;
            
            parse_double_fast(v, dv);
            t->mem_block.num_cols[t->mem_block.col_num_idx[c]].push_back(dv);
        } else {
            t->mem_block.str_cols[t->mem_block.col_str_idx[c]].push_back(std::move(v));
        }
    };

    
    ifstream data_bin(data_path(db_name, table_name), ios::binary);
    bool loaded = false;
    if (data_bin) {
        uint32_t magic = 0;
        
        if (read_u32(data_bin, magic) && (magic == kDataMagicV2 || magic == kDataMagicV1)) {
            loaded = true;
            if (magic == kDataMagicV2) {
                while (true) {
                    uint32_t page_id = 0;
                    uint32_t row_count = 0;
                    
                    if (!read_u32(data_bin, page_id)) break; 
                    
                    if (!read_u32(data_bin, row_count)) break; 

                    
                    int rows_at_page_start = t->mem_block.num_rows;
                    size_t bulk_idx_at_page_start = bulk_index_entries.size();

                    bool page_ok = true;
                    for (uint32_t slot = 0; slot < row_count; ++slot) {
                        uint8_t tombstone = 0;
                        uint64_t expires = 0;
                        uint32_t col_count = 0;
                        
                        if (!read_u8(data_bin, tombstone) ||
                            
                            !read_u64(data_bin, expires) ||
                            
                            !read_u32(data_bin, col_count) ||
                            col_count != t->columns.size()) {
                            page_ok = false; break;
                        }

                        t->mem_block.num_rows++;
                        t->mem_block.tombstones.push_back(tombstone);
                        t->mem_block.expirations.push_back(expires);

                        double key = 0.0;
                        bool have_key = false;
                        bool col_error = false;
                        for (size_t c = 0; c < t->columns.size(); ++c) {
                            uint32_t len = 0;
                            
                            if (!read_u32(data_bin, len)) { col_error = true; break; }
                            string v(len, '\0');
                            if (len > 0 && !data_bin.read(&v[0], static_cast<streamsize>(len))) {
                                col_error = true; break;
                            }
                            if (tombstone == 0 && static_cast<int>(c) == t->index_col_idx) {
                                
                                have_key = parse_double_fast(v, key);
                            }
                            push_field(c, v);
                        }
                        if (col_error) { page_ok = false; break; }
                        if (have_key) {
                            
                            size_t rid = encode_rid(page_id, slot);
                            bulk_index_entries.emplace_back(key, rid);
                        }
                    }

                    if (!page_ok) {
                        
                        t->mem_block.num_rows = rows_at_page_start;
                        t->mem_block.tombstones.resize(rows_at_page_start);
                        t->mem_block.expirations.resize(rows_at_page_start);
                        for (auto& v : t->mem_block.num_cols) v.resize(rows_at_page_start);
                        for (auto& v : t->mem_block.str_cols) v.resize(rows_at_page_start);
                        bulk_index_entries.resize(bulk_idx_at_page_start);
                        break;
                    }
                }
            } else {
                while (true) {
                    uint8_t tombstone = 0;
                    uint64_t expires = 0;
                    uint32_t col_count = 0;
                    
                    if (!read_u8(data_bin, tombstone)) break;
                    
                    if (!read_u64(data_bin, expires)) break;
                    
                    if (!read_u32(data_bin, col_count)) break;
                    if (col_count != t->columns.size()) break;

                    int row_idx = t->mem_block.num_rows;
                    (void)row_idx;
                    t->mem_block.num_rows++;
                    t->mem_block.tombstones.push_back(tombstone);
                    t->mem_block.expirations.push_back(expires);

                    double key = 0.0;
                    bool have_key = false;
                    bool col_error = false;
                    for (size_t c = 0; c < t->columns.size(); ++c) {
                        uint32_t len = 0;
                        
                        if (!read_u32(data_bin, len)) { col_error = true; break; }
                        string v(len, '\0');
                        if (len > 0 && !data_bin.read(&v[0], static_cast<streamsize>(len))) {
                            col_error = true; break;
                        }
                        if (tombstone == 0 && static_cast<int>(c) == t->index_col_idx) {
                            
                            have_key = parse_double_fast(v, key);
                        }
                        push_field(c, v);
                    }
                    if (col_error) {
                        
                        t->mem_block.num_rows = row_idx;
                        t->mem_block.tombstones.resize(row_idx);
                        t->mem_block.expirations.resize(row_idx);
                        for (auto& v : t->mem_block.num_cols) v.resize(row_idx);
                        for (auto& v : t->mem_block.str_cols) v.resize(row_idx);
                        break;
                    }
                    if (have_key) {
                        
                        bulk_index_entries.emplace_back(key, row_to_rid(static_cast<size_t>(row_idx)));
                    }
                }
            }
        }
    }
    data_bin.close();

    if (!loaded) {
        
        ifstream data_in(data_path(db_name, table_name));
        if (data_in) {
            while (getline(data_in, line)) {
                
                auto parts = split_tab_escaped(line);
                if (parts.size() < 2 + t->columns.size()) continue;

                uint8_t tombstone = static_cast<uint8_t>(atoi(parts[0].c_str()));
                uint64_t expires = static_cast<uint64_t>(strtoull(parts[1].c_str(), nullptr, 10));

                int row_idx = t->mem_block.num_rows;
                t->mem_block.num_rows++;
                t->mem_block.tombstones.push_back(tombstone);
                t->mem_block.expirations.push_back(expires);

                double key = 0.0;
                bool have_key = false;
                for (size_t c = 0; c < t->columns.size(); ++c) {
                    
                    string v = unescape_field(parts[2 + c]);
                    if (tombstone == 0 && static_cast<int>(c) == t->index_col_idx) {
                        
                        have_key = parse_double_fast(v, key);
                    }
                    push_field(c, v);
                }
                if (have_key) {
                    
                    bulk_index_entries.emplace_back(key, row_to_rid(static_cast<size_t>(row_idx)));
                }
            }
        }
    }

    
    {
        int nr = t->mem_block.num_rows;
        if (static_cast<int>(t->mem_block.tombstones.size()) != nr) t->mem_block.tombstones.resize(nr);
        if (static_cast<int>(t->mem_block.expirations.size()) != nr) t->mem_block.expirations.resize(nr);
        for (auto& v : t->mem_block.num_cols) { if (static_cast<int>(v.size()) != nr) v.resize(nr); }
        for (auto& v : t->mem_block.str_cols) { if (static_cast<int>(v.size()) != nr) v.resize(nr); }
    }

    
    
    replay_wal(db_name, table_name, t, bulk_index_entries);

    
    {
        int nr = t->mem_block.num_rows;
        if (static_cast<int>(t->mem_block.tombstones.size()) != nr) t->mem_block.tombstones.resize(nr);
        if (static_cast<int>(t->mem_block.expirations.size()) != nr) t->mem_block.expirations.resize(nr);
        for (auto& v : t->mem_block.num_cols) { if (static_cast<int>(v.size()) != nr) v.resize(nr); }
        for (auto& v : t->mem_block.str_cols) { if (static_cast<int>(v.size()) != nr) v.resize(nr); }
    }

    
    if (!bulk_index_entries.empty()) {
        double max_key = bulk_index_entries[0].first;
        for (auto& [k, _r] : bulk_index_entries) {
            if (k > max_key) max_key = k;
        }
        t->max_indexed_key = max_key;
        t->primary_index.bulkLoad(bulk_index_entries);
    }
    t->pending_index_entries.clear();
    t->binary_files_ready = true;

    lock_guard<mutex> lock(db_mutex);
    databases[db_name][table_name] = t;
    return true;
}


// Iterates the data directory and loads every table in every database.
static void load_all_databases() {
    fs::create_directories(kDataDir);
    for (const auto& db_entry : fs::directory_iterator(kDataDir)) {
        if (!db_entry.is_directory()) continue;
        string db_name = db_entry.path().filename().string();
        
        fs::path schema_dir = db_root(db_name) / "schema";
        if (!fs::exists(schema_dir)) continue;
        for (const auto& entry : fs::directory_iterator(schema_dir)) {
            if (!entry.is_regular_file()) continue;
            string fname = entry.path().filename().string();
            if (fname.size() <= 4 || fname.substr(fname.size() - 4) != ".sch") continue;
            string table_name = fname.substr(0, fname.size() - 4);
            
            load_table_from_disk(db_name, table_name);
        }
    }
}

struct WhereClause {
    bool present = false;
    string left;
    string op;
    string right;
};


// Parses a WHERE clause string into column, operator, and value parts.
static bool parse_where_clause(const string& where_text, WhereClause& wc) {
    static const vector<string> ops = {"<=", ">=", "=", "<", ">"};
    
    string clause = trim_copy(where_text);
    for (const auto& op : ops) {
        auto pos = clause.find(op);
        if (pos == string::npos) continue;
        wc.present = true;
        
        wc.left = trim_copy(clause.substr(0, pos));
        wc.op = op;
        
        wc.right = trim_copy(clause.substr(pos + op.size()));
        
        wc.right = dequote(wc.right);
        return true;
    }
    return false;
}


// Compares two values with the given operator, using numeric comparison when possible.
static bool compare_with_op(const string& l, const string& op, const string& r) {
    double ld = 0.0;
    double rd = 0.0;
    
    bool lnum = parse_double_fast(l, ld);
    
    bool rnum = parse_double_fast(r, rd);

    if (lnum && rnum) {
        if (op == "=") return ld == rd;
        if (op == "<") return ld < rd;
        if (op == ">") return ld > rd;
        if (op == "<=") return ld <= rd;
        if (op == ">=") return ld >= rd;
    } else {
        if (op == "=") return l == r;
        if (op == "<") return l < r;
        if (op == ">") return l > r;
        if (op == "<=") return l <= r;
        if (op == ">=") return l >= r;
    }
    return false;
}


// Uses the B+ tree to find candidate row indices for a WHERE condition.
static vector<size_t> candidate_rows_with_index(TableCache* t, const WhereClause& wc) {
    vector<size_t> ids;
    
    materialize_pending_index(t);
    
    int lhs_idx = resolve_col_idx(t, wc.left);
    if (lhs_idx != t->index_col_idx) return ids;

    double rhs = 0.0;
    
    if (!parse_double_fast(wc.right, rhs)) return ids;

    if (wc.op == "=") {
        auto rids = t->primary_index.search(rhs);
        ids.reserve(rids.size());
        
        for (auto rid : rids) ids.push_back(rid_to_row(rid));
        return ids;
    }

    if (wc.op == "<") {
        auto rids = t->primary_index.searchRange(-numeric_limits<double>::infinity(), rhs, true, false);
        ids.reserve(rids.size());
        
        for (auto rid : rids) ids.push_back(rid_to_row(rid));
        return ids;
    }
    if (wc.op == "<=") {
        auto rids = t->primary_index.searchRange(-numeric_limits<double>::infinity(), rhs, true, true);
        ids.reserve(rids.size());
        
        for (auto rid : rids) ids.push_back(rid_to_row(rid));
        return ids;
    }
    if (wc.op == ">") {
        auto rids = t->primary_index.searchRange(rhs, numeric_limits<double>::infinity(), false, true);
        ids.reserve(rids.size());
        
        for (auto rid : rids) ids.push_back(rid_to_row(rid));
        return ids;
    }
    if (wc.op == ">=") {
        auto rids = t->primary_index.searchRange(rhs, numeric_limits<double>::infinity(), true, true);
        ids.reserve(rids.size());
        
        for (auto rid : rids) ids.push_back(rid_to_row(rid));
        return ids;
    }

    return ids;
}


// Handles CREATE TABLE: parses columns, persists schema, initializes storage.
static void handle_create(const string& db_name, const string& sql, int client_socket) {
    
    string s = trim_copy(sql);
    
    auto up = upper_copy(s);

    auto table_pos = up.find("TABLE ");
    if (table_pos == string::npos) {
        
        send_error(client_socket, "Invalid CREATE");
        return;
    }

    bool if_not_exists = (up.find("IF NOT EXISTS") != string::npos);

    size_t t_start = table_pos + 6;
    if (if_not_exists) {
        auto ine = up.find("IF NOT EXISTS");
        t_start = ine + string("IF NOT EXISTS").size();
    }
    while (t_start < s.size() && isspace(static_cast<unsigned char>(s[t_start]))) t_start++;

    auto open_paren = s.find('(', t_start);
    auto close_paren = s.rfind(')');
    if (open_paren == string::npos || close_paren == string::npos || close_paren <= open_paren) {
        
        send_error(client_socket, "Invalid CREATE syntax");
        return;
    }

    
    string table_name = trim_copy(up.substr(t_start, open_paren - t_start));
    if (table_name.empty()) {
        
        send_error(client_socket, "Missing table name");
        return;
    }

    {
        lock_guard<mutex> lock(db_mutex);
        if (databases[db_name].find(table_name) != databases[db_name].end()) {
            if (!if_not_exists) {
                
                send_error(client_socket, "Table already exists");
                return;
            }
            
            send_response(client_socket, "OK");
            return;
        }
    }

    string cols_def = s.substr(open_paren + 1, close_paren - open_paren - 1);
    
    auto defs = split_csv_top_level(cols_def);
    if (defs.empty()) {
        
        send_error(client_socket, "No columns");
        return;
    }

    auto* t = new TableCache(db_name, table_name);
    t->mem_block.columns.clear();

    int pk_idx = -1;
    for (size_t i = 0; i < defs.size(); ++i) {
        
        string d = trim_copy(defs[i]);
        if (d.empty()) continue;

        auto sp = d.find(' ');
        if (sp == string::npos) {
            delete t;
            
            send_error(client_socket, "Invalid column definition");
            return;
        }

        
        string col = upper_copy(trim_copy(d.substr(0, sp)));
        
        string rest = upper_copy(trim_copy(d.substr(sp + 1)));

        int ctype = 2;
        if (rest.find("INT") != string::npos) ctype = 0;
        else if (rest.find("DECIMAL") != string::npos || rest.find("DOUBLE") != string::npos || rest.find("FLOAT") != string::npos) ctype = 1;
        else if (rest.find("DATETIME") != string::npos) ctype = 2;
        else if (rest.find("VARCHAR") != string::npos) ctype = 2;
        else if (rest.find("TEXT") != string::npos) ctype = 2;
        else if (rest.find("CHAR") != string::npos) ctype = 2;

        if (rest.find("PRIMARY KEY") != string::npos && pk_idx == -1) {
            pk_idx = static_cast<int>(i);
        }

        t->columns.push_back(col);
        t->col_types.push_back(ctype);
    }

    if (t->columns.empty()) {
        delete t;
        
        send_error(client_socket, "No valid columns");
        return;
    }

    
    
    setup_pax_types(t);
    constexpr size_t kPreReserve = 1 << 20;
    t->mem_block.tombstones.reserve(kPreReserve);
    t->mem_block.expirations.reserve(kPreReserve);
    for (auto& v : t->mem_block.num_cols) v.reserve(kPreReserve);
    for (auto& v : t->mem_block.str_cols) v.reserve(kPreReserve);
    t->pending_index_entries.reserve(kPreReserve);
    t->index_col_idx = (pk_idx >= 0 ? pk_idx : 0);
    t->index_col_name = t->columns[t->index_col_idx];

    
    fs::create_directories(table_path(db_name, table_name));
    
    fs::create_directories(index_path(db_name, table_name).parent_path());
    
    if (!ensure_binary_file_header(data_path(db_name, table_name), kDataMagicV2) ||
        
        !ensure_binary_file_header(index_path(db_name, table_name), kIndexMagic)) {
        delete t;
        
        send_error(client_socket, "Failed to initialize table files");
        return;
    }
    t->binary_files_ready = true;

    
    if (!persist_schema(t)) {
        delete t;
        
        send_error(client_socket, "Failed to persist schema");
        return;
    }

    {
        lock_guard<mutex> lock(db_mutex);
        databases[db_name][table_name] = t;
    }

    
    send_response(client_socket, "OK");
}


// Handles INSERT INTO: parses rows, writes to WAL and data file, updates index.
static void handle_insert(const string& db_name, string_view sql, int client_socket,
                          string& data_buf, string& idx_buf,
                          vector<pair<double, size_t>>& index_batch) {
    
    string_view s = trim_view(sql);
    
    auto into_pos = find_ci_view(s, "INTO ");
    
    auto values_pos = find_ci_view(s, " VALUES");
    if (into_pos == string::npos || values_pos == string::npos || values_pos <= into_pos + 5) {
        
        send_error(client_socket, "Invalid INSERT");
        return;
    }

    
    string table_name = upper_copy(trim_copy(string(s.substr(into_pos + 5, values_pos - (into_pos + 5)))));

    TableCache* t = nullptr;
    {
        lock_guard<mutex> lock(db_mutex);
        auto it = databases[db_name].find(table_name);
        if (it != databases[db_name].end()) t = it->second;
    }
    if (!t) {
        
        send_error(client_socket, "Table not found");
        return;
    }

    string_view values_part = s.substr(values_pos + 7);
    
    size_t estimated_rows = values_part.size() / 32;
    if (estimated_rows == 0) estimated_rows = 1;

    unique_lock<shared_mutex> lock(t->rw_mutex);
    
    uint64_t now = current_time_sec();

    
    int expires_col_idx = resolve_col_idx(t, "EXPIRES_AT");

    if (!t->binary_files_ready) {
        
        if (!ensure_binary_file_header(data_path(db_name, table_name), kDataMagicV2) ||
            
            !ensure_binary_file_header(index_path(db_name, table_name), kIndexMagic)) {
            
            if (!rewrite_table_binary_files(t)) {
                
                send_error(client_socket, "Failed to migrate table files to binary");
                return;
            }
        }
        t->binary_files_ready = true;
    }

    
    data_buf.clear();
    size_t data_est = estimated_rows * 96 + 128;
    if (data_buf.capacity() < data_est) data_buf.reserve(data_est);
    data_buf.resize(data_buf.capacity());
    char* dwp = data_buf.data();

    uint32_t open_page_id = UINT32_MAX;
    size_t open_count_offset = 0;
    uint32_t open_count = 0;

    auto start_page_chunk = [&](uint32_t page_id) {
        open_page_id = page_id;
        memcpy(dwp, &page_id, 4); dwp += 4;
        open_count_offset = static_cast<size_t>(dwp - data_buf.data());
        dwp += 4;
        open_count = 0;
    };

    auto close_page_chunk = [&]() {
        if (open_page_id != UINT32_MAX)
            memcpy(data_buf.data() + open_count_offset, &open_count, 4);
    };

    
    size_t cur_rows = static_cast<size_t>(t->mem_block.num_rows);
    size_t target = cur_rows + estimated_rows;
    if (t->mem_block.tombstones.capacity() < target) t->mem_block.tombstones.reserve(target * 2);
    if (t->mem_block.expirations.capacity() < target) t->mem_block.expirations.reserve(target * 2);
    for (auto& v : t->mem_block.num_cols) { if (v.capacity() < target) v.reserve(target * 2); }
    for (auto& v : t->mem_block.str_cols) { if (v.capacity() < target) v.reserve(target * 2); }

    const size_t ncols = t->columns.size();
    bool pax_active = !t->mem_block.col_is_numeric.empty();

    
    index_batch.clear();
    index_batch.reserve(estimated_rows);

    
    uint32_t first_new_page = UINT32_MAX;
    uint32_t last_new_page = 0;

    
    const int saved_num_rows = t->mem_block.num_rows;
    const size_t saved_tomb_size = t->mem_block.tombstones.size();
    const size_t saved_exp_size = t->mem_block.expirations.size();
    vector<size_t> saved_num_sizes, saved_str_sizes;
    for (auto& v : t->mem_block.num_cols) saved_num_sizes.push_back(v.size());
    for (auto& v : t->mem_block.str_cols) saved_str_sizes.push_back(v.size());
    bool duplicate_pk = false;
    bool batch_ascending = true;
    double prev_batch_key = -1e300;

    
    bool parse_ok = for_each_insert_group_sv(values_part, ncols,
        [&](const string_view* row, size_t row_size) -> bool {
        if (row_size != ncols) {
            
            send_error(client_socket, "Column count mismatch");
            return false;
        }

        
        if (__builtin_expect(
            static_cast<size_t>(data_buf.data() + data_buf.size() - dwp) < 8192, 0)) {
            size_t pos = static_cast<size_t>(dwp - data_buf.data());
            data_buf.resize(data_buf.size() * 2);
            dwp = data_buf.data() + pos;
        }

        uint64_t expires = now + 31536000ULL;
        if (expires_col_idx >= 0) {
            double ev = 0.0;
            
            if (parse_double_fast_sv(row[expires_col_idx], ev) && ev > 0.0)
                expires = static_cast<uint64_t>(ev);
        }

        int row_idx = t->mem_block.num_rows;
        t->mem_block.num_rows++;
        t->mem_block.tombstones.push_back(0);
        t->mem_block.expirations.push_back(expires);

        
        uint32_t page_id = static_cast<uint32_t>(row_idx / static_cast<int>(kRowsPerPage));
        if (open_page_id != page_id) { close_page_chunk(); start_page_chunk(page_id); }

        *dwp++ = 0;           
        memcpy(dwp, &expires, 8); dwp += 8;
        { uint32_t nc = static_cast<uint32_t>(ncols); memcpy(dwp, &nc, 4); dwp += 4; }

        if (pax_active) {
            for (size_t c = 0; c < ncols; ++c) {
                const string_view& tok = row[c];
                if (t->mem_block.col_is_numeric[c]) {
                    double v = 0.0;
                    
                    parse_double_fast_sv(tok, v);
                    t->mem_block.num_cols[t->mem_block.col_num_idx[c]].push_back(v);
                } else {
                    t->mem_block.str_cols[t->mem_block.col_str_idx[c]].emplace_back(
                        tok.data(), tok.size());
                }
                uint32_t tl = static_cast<uint32_t>(tok.size());
                memcpy(dwp, &tl, 4); dwp += 4;
                if (tl) { memcpy(dwp, tok.data(), tl); dwp += tl; }
            }
        } else {
            for (size_t c = 0; c < ncols; ++c) {
                const string_view& tok = row[c];
                t->mem_block.columns[c].emplace_back(tok.data(), tok.size());
                uint32_t tl = static_cast<uint32_t>(tok.size());
                memcpy(dwp, &tl, 4); dwp += 4;
                if (tl) { memcpy(dwp, tok.data(), tl); dwp += tl; }
            }
        }

        open_count++;

        
        if (page_id < first_new_page) first_new_page = page_id;
        if (page_id > last_new_page)  last_new_page  = page_id;

        
        double key = 0.0;
        bool have_key = false;
        if (pax_active && t->mem_block.col_is_numeric[t->index_col_idx]) {
            int ni = t->mem_block.col_num_idx[t->index_col_idx];
            key = t->mem_block.num_cols[ni].back();
            have_key = true;
        } else {
            
            have_key = parse_double_fast_sv(row[t->index_col_idx], key);
        }
        if (have_key) {
            if (key <= prev_batch_key) batch_ascending = false;
            prev_batch_key = key;
            
            size_t rid = row_to_rid(static_cast<size_t>(row_idx));
            index_batch.emplace_back(key, rid);
        }
        return true;
    });

    if (!parse_ok || duplicate_pk) {
        
        t->mem_block.num_rows = saved_num_rows;
        t->mem_block.tombstones.resize(saved_tomb_size);
        t->mem_block.expirations.resize(saved_exp_size);
        for (size_t i = 0; i < t->mem_block.num_cols.size(); ++i)
            t->mem_block.num_cols[i].resize(saved_num_sizes[i]);
        for (size_t i = 0; i < t->mem_block.str_cols.size(); ++i)
            t->mem_block.str_cols[i].resize(saved_str_sizes[i]);
        
        if (duplicate_pk) send_error(client_socket, "Duplicate primary key");
        return;
    }

    
    if (!index_batch.empty()) {
        if (batch_ascending && index_batch.front().first > t->max_indexed_key) {
            
            
            goto dedup_done;
        }
        {
        
        bool sorted = batch_ascending;
        if (!sorted) {
            sorted = true;
            for (size_t i = 1; i < index_batch.size(); ++i) {
                if (index_batch[i].first == index_batch[i - 1].first) {
                    duplicate_pk = true; break;
                }
                if (index_batch[i].first < index_batch[i - 1].first) {
                    sorted = false; break;
                }
            }
        }
        if (!duplicate_pk && !sorted) {
            sort(index_batch.begin(), index_batch.end());
            for (size_t i = 1; i < index_batch.size(); ++i) {
                if (index_batch[i].first == index_batch[i - 1].first) {
                    duplicate_pk = true; break;
                }
            }
        }
        
        if (!duplicate_pk && saved_num_rows > 0) {
            double bmin = index_batch.front().first;
            if (bmin > t->max_indexed_key) goto dedup_done;
            for (const auto& [key, _rid] : index_batch) {
                if (key > t->max_indexed_key) break;
                auto hits = t->primary_index.search(key);
                for (size_t existing_rid : hits) {
                    
                    int row = static_cast<int>(rid_page(existing_rid)) * static_cast<int>(kRowsPerPage)
                            
                            + static_cast<int>(rid_slot(existing_rid));
                    if (row < saved_num_rows && t->mem_block.tombstones[row] == 0) {
                        duplicate_pk = true; break;
                    }
                }
                if (duplicate_pk) break;
            }
        }
        }
    dedup_done:
        if (duplicate_pk) {
            t->mem_block.num_rows = saved_num_rows;
            t->mem_block.tombstones.resize(saved_tomb_size);
            t->mem_block.expirations.resize(saved_exp_size);
            for (size_t i = 0; i < t->mem_block.num_cols.size(); ++i)
                t->mem_block.num_cols[i].resize(saved_num_sizes[i]);
            for (size_t i = 0; i < t->mem_block.str_cols.size(); ++i)
                t->mem_block.str_cols[i].resize(saved_str_sizes[i]);
            
            send_error(client_socket, "Duplicate primary key");
            return;
        }
        
        double batch_max = index_batch.back().first;
        if (batch_max > t->max_indexed_key) t->max_indexed_key = batch_max;
    }

    close_page_chunk();
    data_buf.resize(static_cast<size_t>(dwp - data_buf.data()));

    
    if (first_new_page != UINT32_MAX && !t->page_cache_rows.empty()) {
        lock_guard<mutex> cache_lock(t->page_cache_mutex);
        for (uint32_t pg = first_new_page; pg <= last_new_page; ++pg) {
            auto it = t->page_cache_pos.find(pg);
            if (it != t->page_cache_pos.end()) {
                t->page_cache_lru.erase(it->second);
                t->page_cache_pos.erase(it);
                t->page_cache_rows.erase(pg);
                t->page_scan_touch.erase(pg);
            }
        }
    }

    
    
    
    
    

    
    t->primary_index.bulkInsertUnlocked(index_batch);

    
    {
        lock_guard<mutex> stream_lock(t->append_stream_mutex);
        
        if (!ensure_append_streams_open(t)) {
            
            send_error(client_socket, "Failed to open data file");
            return;
        }
        
        if (t->wal_fd >= 0 && !data_buf.empty()) {
            
            write_wal_entry(t->wal_fd,
                            static_cast<uint32_t>(saved_num_rows),
                            static_cast<uint32_t>(t->mem_block.num_rows - saved_num_rows),
                            data_buf.data(), data_buf.size());
        }
        
        if (!data_buf.empty()) {
            size_t written = 0;
            while (written < data_buf.size()) {
                ssize_t n = ::write(t->data_append_fd,
                                    data_buf.data() + written,
                                    data_buf.size() - written);
                if (n <= 0) break;
                written += static_cast<size_t>(n);
            }
        }
        
        if (++t->wal_batch_count >= kWalCheckpointInterval) {
            
            truncate_wal(t->wal_fd);
            t->wal_batch_count = 0;
        }
    }

    
    lock.unlock();
    
    send_response(client_socket, "OK");
}


// Handles DELETE FROM: tombstones matching rows and rewrites data files.
static void handle_delete(const string& db_name, const string& sql, int client_socket) {
    
    string s = trim_copy(sql);
    
    auto up = upper_copy(s);

    auto from_pos = up.find("FROM ");
    if (from_pos == string::npos) {
        
        send_error(client_socket, "Invalid DELETE");
        return;
    }

    
    size_t t_start = from_pos + 5;
    size_t where_pos = up.find(" WHERE ", t_start);
    size_t semi_pos = s.find(';', t_start);
    size_t t_end;
    if (where_pos != string::npos) t_end = where_pos;
    else if (semi_pos != string::npos) t_end = semi_pos;
    else t_end = s.size();
    
    string table_name = trim_copy(up.substr(t_start, t_end - t_start));

    TableCache* t = nullptr;
    {
        lock_guard<mutex> lock(db_mutex);
        auto it = databases[db_name].find(table_name);
        if (it != databases[db_name].end()) t = it->second;
    }
    if (!t) {
        
        send_error(client_socket, "Table not found");
        return;
    }

    
    WhereClause wc;
    if (where_pos != string::npos) {
        size_t where_start = where_pos + 7;
        size_t where_end = (semi_pos != string::npos) ? semi_pos : s.size();
        
        parse_where_clause(s.substr(where_start, where_end - where_start), wc);
    }

    unique_lock<shared_mutex> lock(t->rw_mutex);

    if (!wc.present) {
        
        t->mem_block.num_rows = 0;
        t->mem_block.tombstones.clear();
        t->mem_block.expirations.clear();
        for (auto& c : t->mem_block.columns) c.clear();
        for (auto& c : t->mem_block.num_cols)  c.clear();
        for (auto& c : t->mem_block.str_cols)  c.clear();
        t->primary_index.reset();
        t->max_indexed_key = -1e300;
        t->pending_index_entries.clear();
        
        page_cache_clear(t);
        t->binary_files_ready = true;
        lock.unlock();

        {
            lock_guard<mutex> stream_lock(t->append_stream_mutex);
            
            close_append_streams(t);
        }
        
        ofstream(data_path(db_name, table_name), ios::binary | ios::trunc).close();
        
        ofstream(index_path(db_name, table_name), ios::binary | ios::trunc).close();
        
        ensure_binary_file_header(data_path(db_name, table_name), kDataMagicV2);
        
        ensure_binary_file_header(index_path(db_name, table_name), kIndexMagic);
        
        {
            
            auto wp = wal_path(db_name, table_name);
            int wfd = open(wp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (wfd >= 0) {
                uint32_t m = kWalMagic;
                ::write(wfd, &m, sizeof(m));
                ::close(wfd);
            }
        }
    } else {
        
        
        uint64_t now = current_time_sec();

        
        
        int wc_col_pre = resolve_col_idx(t, wc.left);
        bool wc_col_numeric = false;
        int wc_col_num_idx = -1;
        double wc_rhs_d = 0.0;
        
        bool wc_rhs_numeric = parse_double_fast(wc.right, wc_rhs_d);
        if (wc_col_pre >= 0 && !t->mem_block.col_is_numeric.empty() &&
                t->mem_block.col_is_numeric[static_cast<size_t>(wc_col_pre)]) {
            wc_col_numeric = true;
            wc_col_num_idx = t->mem_block.col_num_idx[static_cast<size_t>(wc_col_pre)];
        }

        auto row_matches_wc = [&](int i) -> bool {
            if (i < 0 || i >= t->mem_block.num_rows) return false;
            if (t->mem_block.tombstones[i] != 0) return false;
            if (t->mem_block.expirations[i] < now) return false;
            if (wc_col_numeric && wc_rhs_numeric) {
                double lhs = t->mem_block.num_cols[static_cast<size_t>(wc_col_num_idx)][static_cast<size_t>(i)];
                const string& op = wc.op;
                if (op[0] == '=') return lhs == wc_rhs_d;
                if (op[0] == '<') return op.size() == 1 ? lhs < wc_rhs_d : lhs <= wc_rhs_d;
                if (op[0] == '>') return op.size() == 1 ? lhs > wc_rhs_d : lhs >= wc_rhs_d;
            }
            string l;
            
            if (wc_col_pre >= 0) l = cell_str(t, wc_col_pre, i);
            
            else l = dequote(wc.left);
            
            return compare_with_op(l, wc.op, wc.right);
        };

        
        int dst = 0;
        int total = t->mem_block.num_rows;
        for (int src = 0; src < total; ++src) {
            if (row_matches_wc(src)) continue; 
            if (dst != src) {
                t->mem_block.tombstones[dst] = t->mem_block.tombstones[src];
                t->mem_block.expirations[dst] = t->mem_block.expirations[src];
                for (auto& v : t->mem_block.num_cols)
                    v[dst] = v[src];
                for (auto& v : t->mem_block.str_cols)
                    v[dst] = std::move(v[src]);
            }
            dst++;
        }
        t->mem_block.num_rows = dst;
        t->mem_block.tombstones.resize(dst);
        t->mem_block.expirations.resize(dst);
        for (auto& v : t->mem_block.num_cols) v.resize(dst);
        for (auto& v : t->mem_block.str_cols) v.resize(dst);

        
        t->primary_index.reset();
        t->max_indexed_key = -1e300;
        t->pending_index_entries.clear();
        vector<pair<double, size_t>> idx_entries;
        idx_entries.reserve(dst);
        bool idx_numeric = !t->mem_block.col_is_numeric.empty() &&
                           t->mem_block.col_is_numeric[static_cast<size_t>(t->index_col_idx)];
        for (int i = 0; i < dst; ++i) {
            if (t->mem_block.tombstones[i] != 0) continue;
            double key = 0.0;
            if (idx_numeric) {
                key = t->mem_block.num_cols[t->mem_block.col_num_idx[t->index_col_idx]][i];
            } else {
                
                string ks = cell_str(t, t->index_col_idx, i);
                
                if (!parse_double_fast(ks, key)) continue;
            }
            
            idx_entries.emplace_back(key, row_to_rid(static_cast<size_t>(i)));
            if (key > t->max_indexed_key) t->max_indexed_key = key;
        }
        if (!idx_entries.empty()) t->primary_index.bulkLoad(idx_entries);

        
        page_cache_clear(t);
        t->binary_files_ready = true;
        lock.unlock();

        
        {
            lock_guard<mutex> stream_lock(t->append_stream_mutex);
            
            close_append_streams(t);
        }
        
        shared_lock<shared_mutex> rlock(t->rw_mutex);
        
        rewrite_table_binary_files(t);
        
        {
            
            auto wp = wal_path(db_name, table_name);
            int wfd = open(wp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (wfd >= 0) {
                uint32_t m = kWalMagic;
                ::write(wfd, &m, sizeof(m));
                ::close(wfd);
            }
        }
    }

    
    send_response(client_socket, "OK");
}


// Resolves a column expression to its string value for a specific row.
static bool resolve_value_single(TableCache* t, int row_idx, const string& expr, string& out) {
    
    string e = trim_copy(expr);
    
    int idx = resolve_col_idx(t, e);
    if (idx >= 0) {
        
        out = cell_str(t, idx, row_idx);
        return true;
    }
    
    out = dequote(e);
    return true;
}


// Handles SELECT with no JOIN: applies WHERE, ORDER BY, LIMIT, sends results.
static void handle_select_no_join(const string& db_name, const string& s, int client_socket) {
    
    auto up = upper_copy(s);

    auto select_pos = up.find("SELECT ");
    auto from_pos = up.find(" FROM ");
    if (select_pos == string::npos || from_pos == string::npos) {
        
        send_error(client_socket, "Invalid SELECT");
        return;
    }

    
    string cols_part = trim_copy(s.substr(select_pos + 7, from_pos - (select_pos + 7)));

    size_t table_start = from_pos + 6;
    size_t where_pos = up.find(" WHERE ", table_start);
    size_t order_pos = up.find(" ORDER BY ", table_start);
    size_t limit_pos = up.find(" LIMIT ", table_start);
    size_t semi_pos = s.find(';', table_start);

    size_t table_end = string::npos;
    if (where_pos != string::npos) table_end = where_pos;
    else if (order_pos != string::npos) table_end = order_pos;
    else if (limit_pos != string::npos) table_end = limit_pos;
    else if (semi_pos != string::npos) table_end = semi_pos;
    else table_end = s.size();

    
    string table_name = trim_copy(up.substr(table_start, table_end - table_start));

    TableCache* t = nullptr;
    {
        lock_guard<mutex> lock(db_mutex);
        auto it = databases[db_name].find(table_name);
        if (it != databases[db_name].end()) t = it->second;
    }
    if (!t) {
        
        send_error(client_socket, "Table not found");
        return;
    }

    vector<int> selected_cols;
    vector<string> selected_names;

    if (cols_part == "*") {
        for (int i = 0; i < static_cast<int>(t->columns.size()); ++i) {
            selected_cols.push_back(i);
            selected_names.push_back(t->columns[i]);
        }
    } else {
        
        auto cols = split_csv_top_level(cols_part);
        for (auto& c : cols) {
            
            int idx = resolve_col_idx(t, c);
            if (idx < 0) {
                
                send_error(client_socket, "Unknown column in SELECT");
                return;
            }
            selected_cols.push_back(idx);
            selected_names.push_back(t->columns[idx]);
        }
    }

    WhereClause wc;
    if (where_pos != string::npos) {
        size_t where_start = where_pos + 7;
        size_t where_end = (order_pos != string::npos ? order_pos : (semi_pos != string::npos ? semi_pos : s.size()));
        
        parse_where_clause(s.substr(where_start, where_end - where_start), wc);
    }

    
    string order_by_col;
    bool order_by_desc = false;
    if (order_pos != string::npos) {
        size_t ob_start = order_pos + 10; 
        size_t ob_end = (semi_pos != string::npos ? semi_pos : s.size());
        
        string ob_clause = trim_copy(s.substr(ob_start, ob_end - ob_start));
        
        string ob_upper = upper_copy(ob_clause);
        if (ob_upper.size() >= 4 && ob_upper.substr(ob_upper.size() - 4) == "DESC") {
            order_by_desc = true;
            
            ob_clause = trim_copy(ob_clause.substr(0, ob_clause.size() - 4));
        } else if (ob_upper.size() >= 3 && ob_upper.substr(ob_upper.size() - 3) == "ASC") {
            
            ob_clause = trim_copy(ob_clause.substr(0, ob_clause.size() - 3));
        }
        
        order_by_col = trim_copy(ob_clause);
    }

    
    int limit_n = -1;  
    if (limit_pos != string::npos) {
        size_t lim_start = limit_pos + 7; 
        size_t lim_end = (semi_pos != string::npos ? semi_pos : s.size());
        
        string lim_str = trim_copy(s.substr(lim_start, lim_end - lim_start));
        double lv = 0.0;
        
        if (parse_double_fast(lim_str, lv) && lv >= 0)
            limit_n = static_cast<int>(lv);
    }

    shared_lock<shared_mutex> lock(t->rw_mutex);

    vector<size_t> candidates;
    bool used_index = false;
    if (wc.present) {
        
        candidates = candidate_rows_with_index(t, wc);
        
        if (!candidates.empty() || (resolve_col_idx(t, wc.left) == t->index_col_idx)) {
            used_index = true;
        }
    }

    string payload;
    auto emit_header = [&]() {
        string h;
        for (size_t i = 0; i < selected_names.size(); ++i) {
            h += selected_names[i];
            if (i + 1 < selected_names.size()) h += "|";
        }
        h += "\n";
        payload += h;
    };
    emit_header();

    
    uint64_t now = current_time_sec();

    
    int    wc_col_pre       = -1;
    bool   wc_col_numeric   = false;
    int    wc_col_num_idx   = -1;
    double wc_rhs_d         = 0.0;
    bool   wc_rhs_numeric   = false;
    if (wc.present) {
        
        wc_col_pre     = resolve_col_idx(t, wc.left);
        
        wc_rhs_numeric = parse_double_fast(wc.right, wc_rhs_d);
        if (wc_col_pre >= 0 && !t->mem_block.col_is_numeric.empty() &&
                t->mem_block.col_is_numeric[static_cast<size_t>(wc_col_pre)]) {
            wc_col_numeric = true;
            wc_col_num_idx = t->mem_block.col_num_idx[static_cast<size_t>(wc_col_pre)];
        }
    }

    auto row_matches = [&](int i) -> bool {
        if (i < 0 || i >= t->mem_block.num_rows) return false;
        if (t->mem_block.tombstones[i] != 0) return false;
        if (t->mem_block.expirations[i] < now) return false;

        if (wc.present) {
            
            if (wc_col_numeric && wc_rhs_numeric) {
                double lhs = t->mem_block.num_cols[static_cast<size_t>(wc_col_num_idx)][static_cast<size_t>(i)];
                const string& op = wc.op;
                if (op[0] == '=') return lhs == wc_rhs_d;
                if (op[0] == '<') return op.size() == 1 ? lhs < wc_rhs_d : lhs <= wc_rhs_d;
                if (op[0] == '>') return op.size() == 1 ? lhs > wc_rhs_d : lhs >= wc_rhs_d;
            }
            string l;
            
            resolve_value_single(t, i, wc.left, l);
            
            if (!compare_with_op(l, wc.op, wc.right)) return false;
        }

        return true;
    };

    
    const bool pax_ready = !t->mem_block.col_is_numeric.empty();
    int emitted_rows = 0;
    auto append_row = [&](int i, string& out) {
        if (limit_n >= 0 && emitted_rows >= limit_n) return;
        if (!row_matches(i)) return;

        for (size_t c = 0; c < selected_cols.size(); ++c) {
            int col = selected_cols[c];
            if (pax_ready && t->mem_block.col_is_numeric[static_cast<size_t>(col)]) {
                double v = t->mem_block.num_cols[static_cast<size_t>(
                    t->mem_block.col_num_idx[static_cast<size_t>(col)])][static_cast<size_t>(i)];
                long long iv = static_cast<long long>(v);
                if (static_cast<double>(iv) == v &&
                        v >= -9.007199254740992e15 && v <= 9.007199254740992e15) {
                    char buf[22]; char* p = buf + 21; *p = '\0';
                    bool neg = iv < 0;
                    unsigned long long u = neg
                        ? static_cast<unsigned long long>(-(iv + 1)) + 1ULL
                        : static_cast<unsigned long long>(iv);
                    if (u == 0) { *--p = '0'; }
                    else { while (u) { *--p = '0' + static_cast<char>(u % 10); u /= 10; } }
                    if (neg) *--p = '-';
                    out.append(p, static_cast<size_t>(buf + 21 - p));
                } else {
                    char tmp[32];
                    int n = snprintf(tmp, sizeof(tmp), "%.15g", v);
                    out.append(tmp, static_cast<size_t>(n > 0 ? n : 0));
                }
            } else if (pax_ready) {
                out += t->mem_block.str_cols[static_cast<size_t>(
                    t->mem_block.col_str_idx[static_cast<size_t>(col)])][static_cast<size_t>(i)];
            } else {
                
                out += cell_str(t, col, i);
            }
            if (c + 1 < selected_cols.size()) out += '|';
        }
        out += '\n';
        emitted_rows++;
    };

    if (!order_by_col.empty()) {
        
        
        int ob_col = resolve_col_idx(t, order_by_col);
        vector<int> sorted_rows;
        if (used_index) {
            sorted_rows.reserve(candidates.size());
            for (size_t id : candidates) {
                if (row_matches(static_cast<int>(id))) sorted_rows.push_back(static_cast<int>(id));
            }
        } else {
            sorted_rows.reserve(static_cast<size_t>(t->mem_block.num_rows));
            for (int i = 0; i < t->mem_block.num_rows; ++i) {
                if (row_matches(i)) sorted_rows.push_back(i);
            }
        }
        if (ob_col >= 0 && !t->mem_block.col_is_numeric.empty()) {
            if (t->mem_block.col_is_numeric[static_cast<size_t>(ob_col)]) {
                int ni = t->mem_block.col_num_idx[static_cast<size_t>(ob_col)];
                const auto& cd = t->mem_block.num_cols[static_cast<size_t>(ni)];
                if (order_by_desc)
                    sort(sorted_rows.begin(), sorted_rows.end(), [&](int a, int b){ return cd[a] > cd[b]; });
                else
                    sort(sorted_rows.begin(), sorted_rows.end(), [&](int a, int b){ return cd[a] < cd[b]; });
            } else {
                int si = t->mem_block.col_str_idx[static_cast<size_t>(ob_col)];
                const auto& cd = t->mem_block.str_cols[static_cast<size_t>(si)];
                if (order_by_desc)
                    sort(sorted_rows.begin(), sorted_rows.end(), [&](int a, int b){ return cd[a] > cd[b]; });
                else
                    sort(sorted_rows.begin(), sorted_rows.end(), [&](int a, int b){ return cd[a] < cd[b]; });
            }
        }
        for (int row_id : sorted_rows) {
            append_row(row_id, payload);
            if (payload.size() >= 64000) {
                
                send_response(client_socket, payload);
                payload.clear();
            }
        }
    } else if (used_index) {
        unordered_map<uint32_t, vector<int>> by_page;
        by_page.reserve(candidates.size() * 2 + 1);
        for (size_t id : candidates) {
            uint32_t page_id = static_cast<uint32_t>(id / kRowsPerPage);
            by_page[page_id].push_back(static_cast<int>(id));
        }
        for (auto& it : by_page) {
            
            shared_lock<shared_mutex> pg_lock(t->page_locks[page_lock_idx(it.first)]);
            for (int row_id : it.second) {
                append_row(row_id, payload);
                if (payload.size() >= 64000) {
                    
                    send_response(client_socket, payload);
                    payload.clear();
                }
            }
        }
    } else {
        const int total_rows = t->mem_block.num_rows;
        const int scan_threads = min(3, max(1, static_cast<int>(thread::hardware_concurrency())));

        if (scan_threads >= 2 && total_rows >= 200000) {
            vector<string> chunk_payloads(scan_threads);
            vector<thread> workers;
            workers.reserve(scan_threads);

            int chunk = (total_rows + scan_threads - 1) / scan_threads;
            for (int w = 0; w < scan_threads; ++w) {
                int start = w * chunk;
                int end = min(total_rows, start + chunk);
                workers.emplace_back([&, w, start, end]() {
                    auto& local = chunk_payloads[w];
                    local.reserve(1 << 20);
                    int i = start;
                    uint32_t prev_page = UINT32_MAX;
                    while (i < end) {
                        uint32_t page_id = static_cast<uint32_t>(i / static_cast<int>(kRowsPerPage));
                        int page_end = min(end, static_cast<int>((static_cast<size_t>(page_id) + 1) * kRowsPerPage));
                        
                        shared_lock<shared_mutex> pg_lock(t->page_locks[page_lock_idx(page_id)]);
                        bool sequential = (prev_page != UINT32_MAX && page_id == prev_page + 1);
                        
                        const auto& cached_rows = page_cache_get_rows(t, page_id, sequential);
                        if (!cached_rows.empty()) {
                            for (int r : cached_rows) {
                                if (r >= i && r < page_end) append_row(r, local);
                            }
                        } else {
                            for (int r = i; r < page_end; ++r) {
                                append_row(r, local);
                            }
                        }
                        prev_page = page_id;
                        i = page_end;
                    }
                });
            }
            for (auto& th : workers) th.join();

            for (auto& cp : chunk_payloads) {
                if (cp.empty()) continue;
                payload += cp;
                if (payload.size() >= 64000) {
                    
                    send_response(client_socket, payload);
                    payload.clear();
                }
            }
        } else {
            int i = 0;
            uint32_t prev_page = UINT32_MAX;
            while (i < total_rows) {
                uint32_t page_id = static_cast<uint32_t>(i / static_cast<int>(kRowsPerPage));
                int page_end = min(total_rows, static_cast<int>((static_cast<size_t>(page_id) + 1) * kRowsPerPage));
                
                shared_lock<shared_mutex> pg_lock(t->page_locks[page_lock_idx(page_id)]);
                bool sequential = (prev_page != UINT32_MAX && page_id == prev_page + 1);
                
                const auto& cached_rows = page_cache_get_rows(t, page_id, sequential);
                if (!cached_rows.empty()) {
                    for (int r : cached_rows) {
                        if (r < i || r >= page_end) continue;
                        append_row(r, payload);
                        if (payload.size() >= 64000) {
                            
                            send_response(client_socket, payload);
                            payload.clear();
                        }
                    }
                } else {
                    for (int r = i; r < page_end; ++r) {
                        append_row(r, payload);
                        if (payload.size() >= 64000) {
                            
                            send_response(client_socket, payload);
                            payload.clear();
                        }
                    }
                }
                prev_page = page_id;
                i = page_end;
            }
        }
    }

    
    if (!payload.empty()) send_response(client_socket, payload);
    
    send_response(client_socket, "OK");
}

struct JoinPlan {
    string left_table;
    string right_table;
    string left_join_col;
    string right_join_col;
    vector<string> select_exprs;
    WhereClause where;
};


// Parses a SELECT with INNER JOIN into a JoinPlan struct.
static bool parse_join_select(const string& s, JoinPlan& plan) {
    
    string up = upper_copy(s);
    auto select_pos = up.find("SELECT ");
    auto from_pos = up.find(" FROM ");
    auto join_pos = up.find(" INNER JOIN ");
    auto on_pos = up.find(" ON ");

    if (select_pos == string::npos || from_pos == string::npos || join_pos == string::npos || on_pos == string::npos) {
        return false;
    }

    
    string cols_part = trim_copy(s.substr(select_pos + 7, from_pos - (select_pos + 7)));
    
    plan.select_exprs = split_csv_top_level(cols_part);

    
    plan.left_table = trim_copy(s.substr(from_pos + 6, join_pos - (from_pos + 6)));

    size_t right_start = join_pos + string(" INNER JOIN ").size();
    
    plan.right_table = trim_copy(s.substr(right_start, on_pos - right_start));

    size_t cond_start = on_pos + 4;
    size_t where_pos = up.find(" WHERE ", cond_start);
    size_t semi = s.find(';', cond_start);
    size_t cond_end = (where_pos != string::npos ? where_pos : (semi != string::npos ? semi : s.size()));

    
    string on_cond = trim_copy(s.substr(cond_start, cond_end - cond_start));
    auto eq = on_cond.find('=');
    if (eq == string::npos) return false;
    
    plan.left_join_col = trim_copy(on_cond.substr(0, eq));
    
    plan.right_join_col = trim_copy(on_cond.substr(eq + 1));

    if (where_pos != string::npos) {
        size_t where_start = where_pos + 7;
        size_t where_end = (semi != string::npos ? semi : s.size());
        
        parse_where_clause(s.substr(where_start, where_end - where_start), plan.where);
    }

    return true;
}


// Resolves a column expression to its value in the context of a JOIN row.
static bool resolve_join_value(TableCache* lt, int li, const string& lname,
                               TableCache* rt, int ri, const string& rname,
                               const string& expr, string& out) {
    
    string e = trim_copy(expr);
    
    string noq = dequote(e);

    auto dot = e.find('.');
    if (dot != string::npos) {
        
        string tname = trim_copy(e.substr(0, dot));
        
        string cname = trim_copy(e.substr(dot + 1));
        if (tname == lname) {
            
            int idx = resolve_col_idx(lt, cname);
            
            if (idx >= 0) { out = cell_str(lt, idx, li); return true; }
        }
        if (tname == rname) {
            
            int idx = resolve_col_idx(rt, cname);
            
            if (idx >= 0) { out = cell_str(rt, idx, ri); return true; }
        }
        return false;
    }

    
    int lidx = resolve_col_idx(lt, e);
    
    if (lidx >= 0) { out = cell_str(lt, lidx, li); return true; }

    
    int ridx = resolve_col_idx(rt, e);
    
    if (ridx >= 0) { out = cell_str(rt, ridx, ri); return true; }

    out = noq;
    return true;
}


// Determines which joined table a WHERE column belongs to.
static int where_table_affinity(TableCache* lt, const string& lname,
                                TableCache* rt, const string& rname,
                                const string& col_expr) {
    
    string e = trim_copy(col_expr);
    auto dot = e.find('.');
    if (dot != string::npos) {
        
        string tname = trim_copy(e.substr(0, dot));
        if (tname == lname) return 0;
        if (tname == rname) return 1;
        return -1;
    }
    
    if (resolve_col_idx(lt, e) >= 0) return 0;
    
    if (resolve_col_idx(rt, e) >= 0) return 1;
    return -1;
}


// Handles SELECT with INNER JOIN: pairs matching rows and sends results.
static void handle_select_join(const string& db_name, const string& s, int client_socket) {
    JoinPlan plan;
    
    if (!parse_join_select(s, plan)) {
        
        send_error(client_socket, "Invalid JOIN SELECT");
        return;
    }

    TableCache* lt = nullptr;
    TableCache* rt = nullptr;
    {
        lock_guard<mutex> lock(db_mutex);
        auto il = databases[db_name].find(plan.left_table);
        auto ir = databases[db_name].find(plan.right_table);
        if (il != databases[db_name].end()) lt = il->second;
        if (ir != databases[db_name].end()) rt = ir->second;
    }
    if (!lt || !rt) {
        
        send_error(client_socket, "Table not found");
        return;
    }

    shared_lock<shared_mutex> left_lock(lt->rw_mutex, defer_lock);
    shared_lock<shared_mutex> right_lock(rt->rw_mutex, defer_lock);
    lock(left_lock, right_lock);

    
    int l_join_idx = resolve_col_idx(lt, plan.left_join_col);
    
    int r_join_idx = resolve_col_idx(rt, plan.right_join_col);
    if (l_join_idx < 0 || r_join_idx < 0) {
        
        send_error(client_socket, "JOIN column not found");
        return;
    }

    vector<string> headers;
    
    for (const auto& expr : plan.select_exprs) headers.push_back(trim_copy(expr));

    
    
    int where_side = -1;
    int where_col_idx = -1;       
    if (plan.where.present) {
        
        where_side = where_table_affinity(lt, plan.left_table, rt, plan.right_table, plan.where.left);
        
        if (where_side == 0) where_col_idx = resolve_col_idx(lt, plan.where.left);
        
        else if (where_side == 1) where_col_idx = resolve_col_idx(rt, plan.where.left);
    }

    
    uint64_t now = current_time_sec();

    
    vector<int> left_rows;

    if (where_side == 0 && where_col_idx >= 0) {
        WhereClause lwc = plan.where;
        
        lwc.left = unqualify_col(lwc.left);
        
        vector<size_t> candidates = candidate_rows_with_index(lt, lwc);
        if (!candidates.empty()) {
            left_rows.reserve(candidates.size());
            for (size_t r : candidates) {
                int li = static_cast<int>(r);
                if (li >= lt->mem_block.num_rows) continue;
                if (lt->mem_block.tombstones[li] != 0) continue;
                if (lt->mem_block.expirations[li] < now) continue;
                left_rows.push_back(li);
            }
        } else {
            for (int l = 0; l < lt->mem_block.num_rows; ++l) {
                if (lt->mem_block.tombstones[l] != 0) continue;
                if (lt->mem_block.expirations[l] < now) continue;
                
                string cv = cell_str(lt, where_col_idx, l);
                
                if (!compare_with_op(cv, plan.where.op, plan.where.right)) continue;
                left_rows.push_back(l);
            }
        }
    } else {
        left_rows.reserve(lt->mem_block.num_rows);
        for (int l = 0; l < lt->mem_block.num_rows; ++l) {
            if (lt->mem_block.tombstones[l] != 0) continue;
            if (lt->mem_block.expirations[l] < now) continue;
            left_rows.push_back(l);
        }
    }

    
    
    
    unordered_map<string, vector<int>> right_map;

    if (where_side == 1 && where_col_idx >= 0) {
        
        WhereClause rwc = plan.where;
        
        rwc.left = unqualify_col(rwc.left);
        
        vector<size_t> candidates = candidate_rows_with_index(rt, rwc);
        if (!candidates.empty()) {
            for (size_t r : candidates) {
                int ri = static_cast<int>(r);
                if (ri >= rt->mem_block.num_rows) continue;
                if (rt->mem_block.tombstones[ri] != 0) continue;
                if (rt->mem_block.expirations[ri] < now) continue;
                
                right_map[cell_str(rt, r_join_idx, ri)].push_back(ri);
            }
        } else {
            for (int r = 0; r < rt->mem_block.num_rows; ++r) {
                if (rt->mem_block.tombstones[r] != 0) continue;
                if (rt->mem_block.expirations[r] < now) continue;
                
                string cv = cell_str(rt, where_col_idx, r);
                
                if (!compare_with_op(cv, plan.where.op, plan.where.right)) continue;
                
                right_map[cell_str(rt, r_join_idx, r)].push_back(r);
            }
        }
    } else if (where_side == 0 && left_rows.size() < 1000) {
        
        
        unordered_set<string> left_keys;
        
        for (int l : left_rows) left_keys.insert(cell_str(lt, l_join_idx, l));

        
        if (r_join_idx == rt->index_col_idx) {
            
            materialize_pending_index(rt);
            for (const string& key : left_keys) {
                double kv = 0.0;
                
                if (parse_double_fast(key, kv)) {
                    auto rids = rt->primary_index.search(kv);
                    for (auto rid : rids) {
                        
                        int ri = static_cast<int>(rid_to_row(rid));
                        if (ri >= rt->mem_block.num_rows) continue;
                        if (rt->mem_block.tombstones[ri] != 0) continue;
                        if (rt->mem_block.expirations[ri] < now) continue;
                        right_map[key].push_back(ri);
                    }
                }
            }
        } else {
            
            for (int r = 0; r < rt->mem_block.num_rows; ++r) {
                if (rt->mem_block.tombstones[r] != 0) continue;
                if (rt->mem_block.expirations[r] < now) continue;
                
                string rk = cell_str(rt, r_join_idx, r);
                if (left_keys.count(rk)) right_map[rk].push_back(r);
            }
        }
    } else {
        
        right_map.reserve(static_cast<size_t>(rt->mem_block.num_rows) * 2 + 1);
        for (int r = 0; r < rt->mem_block.num_rows; ++r) {
            if (rt->mem_block.tombstones[r] != 0) continue;
            if (rt->mem_block.expirations[r] < now) continue;
            
            right_map[cell_str(rt, r_join_idx, r)].push_back(r);
        }
    }

    
    string payload;
    for (size_t i = 0; i < headers.size(); ++i) {
        payload += headers[i];
        if (i + 1 < headers.size()) payload += "|";
    }
    payload += "\n";

    
    
    bool post_filter = (plan.where.present && where_side == -1);

    for (int l : left_rows) {
        
        auto it = right_map.find(cell_str(lt, l_join_idx, l));
        if (it == right_map.end()) continue;

        for (int r : it->second) {
            if (post_filter) {
                string lv;
                
                if (!resolve_join_value(lt, l, plan.left_table, rt, r, plan.right_table, plan.where.left, lv)) continue;
                
                if (!compare_with_op(lv, plan.where.op, plan.where.right)) continue;
            }

            for (size_t c = 0; c < plan.select_exprs.size(); ++c) {
                string v;
                
                if (!resolve_join_value(lt, l, plan.left_table, rt, r, plan.right_table, plan.select_exprs[c], v)) {
                    
                    send_error(client_socket, "Unknown column in SELECT");
                    return;
                }
                payload += v;
                if (c + 1 < plan.select_exprs.size()) payload += "|";
            }
            payload += "\n";

            if (payload.size() >= 64000) {
                
                send_response(client_socket, payload);
                payload.clear();
            }
        }
    }

    
    if (!payload.empty()) send_response(client_socket, payload);
    
    send_response(client_socket, "OK");
}


// Dispatches a SELECT to the join or no-join handler.
static void handle_select(const string& db_name, const string& sql, int client_socket) {
    
    string s = trim_copy(sql);
    
    string up = upper_copy(s);

    if (up.find(" INNER JOIN ") != string::npos) {
        
        handle_select_join(db_name, s, client_socket);
        return;
    }

    
    handle_select_no_join(db_name, s, client_socket);
}


// Handles CREATE DATABASE: creates the directory structure on disk.
static void handle_create_database(const string& sql, int client_socket) {
    
    string s = trim_copy(sql);
    
    auto up = upper_copy(s);
    
    auto pos = up.find("DATABASE ");
    if (pos == string::npos) {
        
        send_error(client_socket, "Invalid CREATE DATABASE");
        return;
    }
    
    string db_name = trim_copy(s.substr(pos + 9));
    
    if (!db_name.empty() && db_name.back() == ';') db_name.pop_back();
    
    db_name = trim_copy(db_name);
    if (db_name.empty()) {
        
        send_error(client_socket, "Missing database name");
        return;
    }
    
    fs::path root = db_root(db_name);
    if (fs::exists(root)) {
        
        send_error(client_socket, "Database already exists");
        return;
    }
    fs::create_directories(root / "tables");
    fs::create_directories(root / "schema");
    fs::create_directories(root / "index");
    {
        lock_guard<mutex> lock(db_mutex);
        databases[db_name]; 
    }
    
    send_response(client_socket, "OK");
}


// Handles DROP DATABASE: removes all tables and the database directory.
static void handle_drop_database(const string& sql, int client_socket) {
    
    string s = trim_copy(sql);
    
    auto up = upper_copy(s);
    auto pos = up.find("DATABASE ");
    if (pos == string::npos) {
        
        send_error(client_socket, "Invalid DROP DATABASE");
        return;
    }
    
    string db_name = trim_copy(s.substr(pos + 9));
    if (!db_name.empty() && db_name.back() == ';') db_name.pop_back();
    
    db_name = trim_copy(db_name);
    if (db_name.empty()) {
        
        send_error(client_socket, "Missing database name");
        return;
    }
    if (db_name == "default_db") {
        
        send_error(client_socket, "Cannot drop default_db");
        return;
    }
    {
        lock_guard<mutex> lock(db_mutex);
        auto it = databases.find(db_name);
        if (it == databases.end()) {
            
            send_error(client_socket, "Database not found");
            return;
        }
        
        for (auto& [tname, tc] : it->second) {
            {
                unique_lock<shared_mutex> tlock(tc->rw_mutex);
                lock_guard<mutex> slock(tc->append_stream_mutex);
                
                close_append_streams(tc);
            }
            delete tc;
        }
        databases.erase(it);
    }
    
    fs::path root = db_root(db_name);
    if (fs::exists(root)) {
        fs::remove_all(root);
    }
    
    send_response(client_socket, "OK");
}


// Handles DROP TABLE: removes the table from disk and from memory.
static void handle_drop_table(const string& db_name, const string& sql, int client_socket) {
    
    string s = trim_copy(sql);
    
    auto up = upper_copy(s);
    auto pos = up.find("TABLE ");
    if (pos == string::npos) {
        
        send_error(client_socket, "Invalid DROP TABLE");
        return;
    }
    
    string table_name = trim_copy(up.substr(pos + 6));
    if (!table_name.empty() && table_name.back() == ';') table_name.pop_back();
    
    table_name = trim_copy(table_name);
    if (table_name.empty()) {
        
        send_error(client_socket, "Missing table name");
        return;
    }
    TableCache* t = nullptr;
    {
        lock_guard<mutex> lock(db_mutex);
        auto& db = databases[db_name];
        auto it = db.find(table_name);
        if (it == db.end()) {
            
            send_error(client_socket, "Table not found");
            return;
        }
        t = it->second;
        db.erase(it);
    }
    {
        unique_lock<shared_mutex> tlock(t->rw_mutex);
        lock_guard<mutex> slock(t->append_stream_mutex);
        
        close_append_streams(t);
    }
    
    
    auto tp = table_path(db_name, table_name);
    if (fs::exists(tp)) fs::remove_all(tp);
    
    auto sp = schema_path(db_name, table_name);
    if (fs::exists(sp)) fs::remove(sp);
    
    auto ip = index_path(db_name, table_name);
    if (fs::exists(ip)) fs::remove_all(ip.parent_path());
    delete t;
    
    send_response(client_socket, "OK");
}


// Reads SQL from a client socket and dispatches it to the right handler.
static void handle_client(int client_socket) {
    vector<char> big_buf(64 * 1024 * 1024);
    
    string insert_data_buf;
    string insert_idx_buf;
    vector<pair<double, size_t>> insert_index_batch;
    insert_data_buf.reserve(512 * 1024);
    insert_idx_buf.reserve(64 * 1024);
    insert_index_batch.reserve(8192);

    string current_db = "default_db";

    while (true) {
        uint32_t net_len = 0;
        if (recv(client_socket, &net_len, sizeof(net_len), MSG_WAITALL) <= 0) break;

        uint32_t len = ntohl(net_len);
        if (len > big_buf.size()) break;
        if (len == 0) {
            
            send_error(client_socket, "Empty SQL");
            continue;
        }

        if (recv(client_socket, big_buf.data(), len, MSG_WAITALL) <= 0) break;

        string_view qv(big_buf.data(), len);
        
        string_view uq = trim_view(qv);

        
        if (starts_with_ci_view(uq, "INSERT INTO")) {
            
            handle_insert(current_db, qv, client_socket, insert_data_buf, insert_idx_buf, insert_index_batch);
        
        } else if (starts_with_ci_view(uq, "CREATE DATABASE")) {
            string q(qv);
            
            handle_create_database(q, client_socket);
        
        } else if (starts_with_ci_view(uq, "CREATE TABLE")) {
            string q(qv);
            
            handle_create(current_db, q, client_socket);
        
        } else if (starts_with_ci_view(uq, "SELECT")) {
            string q(qv);
            
            handle_select(current_db, q, client_socket);
        
        } else if (starts_with_ci_view(uq, "DELETE FROM")) {
            string q(qv);
            
            handle_delete(current_db, q, client_socket);
        
        } else if (starts_with_ci_view(uq, "DROP DATABASE")) {
            string q(qv);
            
            handle_drop_database(q, client_socket);
        
        } else if (starts_with_ci_view(uq, "DROP TABLE")) {
            string q(qv);
            
            handle_drop_table(current_db, q, client_socket);
        
        } else if (starts_with_ci_view(uq, "USE ")) {
            
            string q = trim_copy(string(uq.substr(4)));
            if (!q.empty() && q.back() == ';') q.pop_back();
            
            q = trim_copy(q);
            
            
            auto uq2 = upper_copy(q);
            if (uq2.substr(0, 9) == "DATABASE ") {
                
                q = trim_copy(q.substr(9));
            }
            if (q.empty()) {
                
                send_error(client_socket, "Missing database name");
            } else {
                lock_guard<mutex> lock(db_mutex);
                
                if (databases.find(q) != databases.end() || fs::exists(db_root(q))) {
                    current_db = q;
                    databases[q]; 
                    
                    send_response(client_socket, "OK");
                } else {
                    
                    send_error(client_socket, "Database not found");
                }
            }
        } else {
            
            send_error(client_socket, "Unrecognized SQL command");
        }
    }

    close(client_socket);
}


// Worker thread loop: picks client sockets from the queue and handles them.
static void worker_loop() {
    while (true) {
        int client_socket = -1;
        {
            unique_lock<mutex> lock(g_client_queue_mutex);
            g_client_queue_cv.wait(lock, [] { return !g_client_queue.empty(); });
            client_socket = g_client_queue.front();
            g_client_queue.pop();
        }

        if (client_socket >= 0) {
            
            handle_client(client_socket);
        }
    }
}


// Entry point: sets up the server socket, spawns worker threads, accepts connections.
int main(int argc, char** argv) {
    int port = 9000;
    if (argc >= 2) port = atoi(argv[1]);

    
    load_all_databases();

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        cerr << "Failed to create socket\n";
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(server_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        cerr << "Bind failed on port " << port << "\n";
        return 1;
    }

    if (listen(server_fd, 256) < 0) {
        cerr << "Listen failed\n";
        return 1;
    }

    cout << "FlexQL server listening on " << port << "\n";

    const int worker_count = min(16, max(4, static_cast<int>(thread::hardware_concurrency())));
    vector<thread> workers;
    workers.reserve(worker_count);
    for (int i = 0; i < worker_count; ++i) {
        workers.emplace_back(worker_loop);
    }

    while (true) {
        int client_socket = accept(server_fd, nullptr, nullptr);
        if (client_socket >= 0) {
            
            int buf_size = 4 * 1024 * 1024;  
            setsockopt(client_socket, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
            setsockopt(client_socket, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
            int no_delay = 1;
            setsockopt(client_socket, IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay));
            int quick_ack = 1;
            setsockopt(client_socket, IPPROTO_TCP, TCP_QUICKACK, &quick_ack, sizeof(quick_ack));
            {
                lock_guard<mutex> lock(g_client_queue_mutex);
                g_client_queue.push(client_socket);
            }
            g_client_queue_cv.notify_one();
        }
    }

    return 0;
}
