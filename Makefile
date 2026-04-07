CXX = g++
CXXFLAGS = -O3 -march=native -flto -std=c++17 -Wall -Wno-unused-result -Wno-unused-function -Iinclude -fPIC
LDFLAGS = -lpthread

all: libflexql.a flexql_server flexql_client benchmark_flexql

libflexql.a: src/flexql.o
	ar rcs $@ $^

src/flexql.o: src/flexql.cpp include/flexql.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/server/server.o: src/server/server.cpp include/cache/lru_cache.h include/index/bptree.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/cache/lru_cache.o: src/cache/lru_cache.cpp include/cache/lru_cache.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

src/index/bptree.o: src/index/bptree.cpp include/index/bptree.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

flexql_server: src/server/server.o src/cache/lru_cache.o src/index/bptree.o
	$(CXX) $(CXXFLAGS) $^ -o $@ $(LDFLAGS)

flexql_client: src/repl.cpp libflexql.a
	$(CXX) $(CXXFLAGS) $< -o $@ -L. -lflexql $(LDFLAGS)

benchmark_flexql: benchmark_flexql.cpp libflexql.a
	$(CXX) $(CXXFLAGS) $< -o $@ -L. -lflexql $(LDFLAGS)

reset-data:
	rm -rf data && mkdir -p data/default_db/{tables,schema,index}

clean:
	rm -f src/*.o src/server/*.o src/cache/*.o src/index/*.o *.o libflexql.a \
	      flexql_server flexql_client benchmark_flexql
