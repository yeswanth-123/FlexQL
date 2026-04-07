#include "flexql.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sstream>
#include <netdb.h>

struct FlexQL {
    int socket_fd;
};


// Frees a memory pointer if it is not null.
extern "C" void flexql_free(void *ptr) {
    if (ptr) free(ptr);
}


// Opens a TCP connection to the FlexQL server at host:port.
extern "C" int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || !db) return FLEXQL_ERROR;
    *db = new FlexQL();
    (*db)->socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if ((*db)->socket_fd < 0) return FLEXQL_ERROR;

    int flag = 1;
    setsockopt((*db)->socket_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int)); 
    int buf_size = 4 * 1024 * 1024;  
    setsockopt((*db)->socket_fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));
    setsockopt((*db)->socket_fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));

    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(port);
    struct addrinfo* result = nullptr;
    if (getaddrinfo(host, port_str.c_str(), &hints, &result) != 0) {
        close((*db)->socket_fd);
        delete *db;
        *db = nullptr;
        return FLEXQL_ERROR;
    }

    bool connected = false;
    for (auto* rp = result; rp != nullptr; rp = rp->ai_next) {
        if (connect((*db)->socket_fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            connected = true;
            break;
        }
    }
    freeaddrinfo(result);

    if (!connected) {
        close((*db)->socket_fd);
        delete *db;
        *db = nullptr;
        return FLEXQL_ERROR;
    }
    return FLEXQL_OK;
}


// Closes the server connection and frees the database handle.
extern "C" int flexql_close(FlexQL *db) {
    if (db) {
        close(db->socket_fd);
        delete db;
        return FLEXQL_OK;
    }
    return FLEXQL_ERROR;
}


// Splits a pipe-delimited row string into a vector of column strings.
static std::vector<std::string> split_row(const std::string& line) {
    std::vector<std::string> cols;
    size_t start = 0;
    while(start < line.length()) {
        size_t end = line.find('|', start);
        if (end == std::string::npos) end = line.length();
        cols.push_back(line.substr(start, end - start));
        start = end + 1;
    }
    return cols;
}

// Sends a SQL query to the server and delivers each result row via callback.
extern "C" int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void*, int, char**, char**), void *arg, char **errmsg) {
    if (!db || !sql) return FLEXQL_ERROR;
    if (errmsg) *errmsg = nullptr;

    
    size_t len = strlen(sql);
    std::vector<char> payload(sizeof(uint32_t) + len);
    uint32_t net_len = htonl(len);
    memcpy(payload.data(), &net_len, sizeof(uint32_t));
    memcpy(payload.data() + sizeof(uint32_t), sql, len);

    
    if (send(db->socket_fd, payload.data(), payload.size(), MSG_NOSIGNAL) < 0) {
        if (errmsg) *errmsg = strdup("ERROR:send failed");
        return FLEXQL_ERROR;
    }

    std::vector<std::string> column_names;
    bool header_parsed = false;

    while (true) {
        uint32_t res_len;
        int n = recv(db->socket_fd, &res_len, sizeof(res_len), MSG_WAITALL);
        if (n <= 0) {
            if (errmsg) *errmsg = strdup("ERROR:connection closed");
            return FLEXQL_ERROR;
        }

        size_t actual_len = ntohl(res_len);
        if (actual_len == 0) break; 

        std::string response;
        
        response.resize(actual_len);
        
        size_t rx = 0;
        while(rx < actual_len) {
            int chunk = recv(db->socket_fd, &response[rx], actual_len - rx, MSG_WAITALL);
            if (chunk <= 0) {
                if (errmsg) *errmsg = strdup("ERROR:connection closed");
                return FLEXQL_ERROR;
            }
            rx += chunk;
        }

        
        if (response == "OK") {
            break; 
        } else if (response.substr(0, 5) == "ERROR") {
            if (errmsg) *errmsg = strdup(response.c_str());
            return FLEXQL_ERROR;
        } else if (callback) {
            size_t pos = 0;
            while (pos < response.length()) {
                size_t nl = response.find('\n', pos);
                if (nl == std::string::npos) nl = response.length();
                if (nl > pos) { 
                    std::string line = response.substr(pos, nl - pos);
                    
                    std::vector<std::string> cols = split_row(line);
                    if (!header_parsed) {
                        column_names = cols;
                        header_parsed = true;
                        pos = nl + 1;
                        continue;
                    }
                    std::vector<char*> c_argv;
                    std::vector<char*> c_colnames;
                    for (auto& c : cols) c_argv.push_back((char*)c.c_str());
                    for (auto& c : column_names) c_colnames.push_back((char*)c.c_str());
                    int cb_rc = callback(arg, cols.size(), c_argv.data(), c_colnames.data());
                    if (cb_rc != 0) {
                        return FLEXQL_OK;
                    }
                }
                pos = nl + 1;
            }
        }
    }
    return FLEXQL_OK;
}
