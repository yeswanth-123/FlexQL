#include "flexql.h"
#include <iostream>
#include <string>
#include <vector>

using namespace std;


// Prints each result-row column as "column = value" to stdout.
int repl_callback(void* arg, int argc, char** argv, char** azColName) {
    for (int i = 0; i < argc; i++) {
        cout << (azColName[i] ? azColName[i] : "?") << " = "
             << (argv[i] ? argv[i] : "NULL") << "\n";
    }
    cout << "\n";
    return 0;
}


// Entry point: sets up the server socket, spawns worker threads, accepts connections.
int main(int argc, char** argv) {
    if (argc < 3) {
        cout << "Usage: " << argv[0] << " <host> <port>\n";
        return 1;
    }
    
    string host = argv[1];
    int port = stoi(argv[2]);
    
    FlexQL* db = nullptr;
    if (flexql_open(host.c_str(), port, &db) != FLEXQL_OK) {
        cout << "Failed to connect to " << host << ":" << port << "\n";
        return 1;
    }
    
    cout << "Connected to FlexQL server\n";
    
    string line;
    while (true) {
        cout << "flexql> ";
        if (!getline(cin, line)) break;
        if (line == ".exit") break;
        if (line.empty()) continue;
        
        char* errmsg = nullptr;
        if (flexql_exec(db, line.c_str(), repl_callback, nullptr, &errmsg) != FLEXQL_OK) {
            cout << "Error: " << (errmsg ? errmsg : "Unknown") << "\n";
            if (errmsg) flexql_free(errmsg);
        } else {
            cout << "Query executed successfully\n";
        }
    }
    
    cout << "Connection closed\n";
    
    flexql_close(db);
    return 0;
}
