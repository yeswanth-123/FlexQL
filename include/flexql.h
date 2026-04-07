#ifndef FLEXQL_H
#define FLEXQL_H

#ifdef __cplusplus
extern "C" {
#endif

// Return code indicating a successful operation.
#define FLEXQL_OK 0
// Return code indicating a failed operation.
#define FLEXQL_ERROR 1

// Opaque handle representing a connection to the FlexQL server.
typedef struct FlexQL FlexQL;

// Opens a TCP connection to the FlexQL server at the given host and port.
int flexql_open(const char *host, int port, FlexQL **db);

// Closes the server connection and frees the database handle.
int flexql_close(FlexQL *db);

// Sends a SQL query to the server; calls callback for each result row.
int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void*, int, char**, char**),
    void *arg,
    char **errmsg
);

// Frees a memory pointer returned by the library (e.g. errmsg).
void flexql_free(void *ptr);

#ifdef __cplusplus
}
#endif

#endif
