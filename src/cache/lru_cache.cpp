#include "cache/lru_cache.h"


// Initializes the table cache with the given database and table name.
TableCache::TableCache(std::string db, std::string name) : db_name(db), table_name(name) {}

// Destructor for TableCache.
TableCache::~TableCache() {}


// No-op stub; persistence is handled by the server write path.
void TableCache::flush_mem_block() {
    
}
