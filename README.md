# FlexQL

FlexQL is a lightweight C++ client-server SQL database engine built for learning, experimentation, and performance benchmarking. It supports a practical SQL subset, persists data on disk, and includes tools for interactive querying and high-volume insert testing.

## Project Overview

FlexQL is split into three main executables:

- flexql_server: runs the database server and query engine.
- flexql_client: interactive CLI client to send SQL to the server.
- benchmark_flexql: benchmark utility with insertion tests and query validation.

Core code areas:

- include/flexql.h: public C API for client connections and query execution.
- src/server/server.cpp: networking, storage, WAL, indexing, and SQL execution.
- src/repl.cpp: interactive terminal client.
- include/index and src/index: B+ tree index implementation.
- include/cache and src/cache: cache components used by the engine.

## Key Features

- Client-server architecture over TCP.
- Persistent on-disk storage under data/default_db.
- Schema, data, index, and WAL-based durability path.
- B+ tree indexing for efficient row lookup.
- Page-level caching with LRU behavior.
- Interactive SQL REPL client.
- Insert benchmark support for large row counts.

Common SQL operations used in this project include CREATE TABLE, INSERT INTO, SELECT, WHERE, and INNER JOIN.

## Build

Run this from the project root:

	make

Build output:

- flexql_server
- flexql_client
- benchmark_flexql

## How To Run

The command flow below follows the same sequence as HOW_TO_RUN.txt.

### 1. Build

	make

### 2. Start the server (Terminal 1)

Keep this terminal dedicated to the server process:

	./flexql_server

Expected message:

	FlexQL server listening on 9000

If port 9000 is already in use:

	fuser -k 9000/tcp
	./flexql_server

### 3. Connect with interactive client (Terminal 2)

Open a new terminal in the same project folder:

	./flexql_client 127.0.0.1 9000

Expected message:

	Connected to FlexQL server

Sample commands:

	flexql> CREATE TABLE DEMO (ID DECIMAL, NAME VARCHAR(32))
	flexql> INSERT INTO DEMO VALUES (1, 'Alice')
	flexql> SELECT * FROM DEMO
	flexql> EXIT

### 4. Stop the server

Press Ctrl+C in Terminal 1, or run:

	fuser -k 9000/tcp

### 5. Clear existing data

Stop the server first, then run:

	rm -rf data
	mkdir -p data/default_db/{tables,schema,index}

### 6. Run benchmark

Terminal 1 (stop server, clear data, restart):

	fuser -k 9000/tcp
	rm -rf data
	mkdir -p data/default_db/{tables,schema,index}
	./flexql_server

Wait for:

	FlexQL server listening on 9000

Terminal 2 (run benchmark):

	./benchmark_flexql 1000000

For a larger run (after clear and restart):

	./benchmark_flexql 10000000

### 7. Clean build artifacts

	make clean

## Optional

You can run the validation suite only with:

	./benchmark_flexql --unit-test
