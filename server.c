#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdbool.h>

// storage (file) types
#define STORAGE_TYPES 3
#define UNSORTED 1
#define SORTED 2
#define BTREE 3

// function prototypes
void evaluateCommands(int connectionfd);
void parseQuery(int connectionfd, char* query);
void create(int connectionfd, char* query);
void createDatabaseDirectoryIfNotPresent(void);

// for error handling and quitting
void raiseError(int connectionfd, char* function, char* exception, char* exception_info);
void quit(int connectionfd);

int main(int argc, char const *argv[])
{
    // socket setup
    int listenfd = 0;  
    int connectionfd = 0;    
    struct sockaddr_in serv_addr;
    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serv_addr, '0', sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(5000); 
    bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 
    listen(listenfd, 10);

    // clear terminal window (for aesthetics)
    printf("\033[2J");
    printf("\033[%d;%dH", 0, 0);

    // welcome message
    printf("=============================== NickDB - Server ===============================\n");
    printf("Waiting for a client to connect....\n");

    // connect to a client
    connectionfd = accept(listenfd, (struct sockaddr*)NULL, NULL);
    if (connectionfd == 0)
    {
        printf("An error occurred with STDIN, please try restarting the server.\n");
        close(connectionfd);
        exit(1);
    }

    // create a database directory (on first run only)
    createDatabaseDirectoryIfNotPresent();

    // print message and begin evaluating queries from the client
    printf("Connection received from file descriptor %d.\n", connectionfd);
    printf("Ready to accept queries from client.\n");
    printf("=====\n");
    evaluateCommands(connectionfd);
}

/*
 *  evaluateCommands()
 *  Receives queries from the client and evaluates them
 */
void evaluateCommands(int connectionfd)
{
    // variables
    char* query;
    int num_chars;
    int storage_a_bytes = 0;
    int storage_b_bytes = 0;
    // int storage_c_bytes = 0;
    bool storage = 1;
    // char* temp_response = "The query worked!\0";
    // int temp_response_len = strlen(temp_response) + 1;

    // receive queries
    while(1)
    {
        // print message
        printf("Query: ");
        fflush(stdout);

        // get query and its length
        while ((storage_a_bytes = recv(connectionfd, &num_chars, sizeof(int), 0)) <= 0);
        query = malloc(num_chars * sizeof(int));
        write(connectionfd, &storage, sizeof(bool));
        while ((storage_b_bytes = recv(connectionfd, query, num_chars, 0)) <= 0);
        printf("%s\n", query);

        // parse query and call appropriate operator
        parseQuery(connectionfd, query);

        // write a message back to the client
        // write(connectionfd, &temp_response_len, sizeof(int));
        // while ((storage_c_bytes = recv(connectionfd, &storage, sizeof(bool), 0)) <= 0);
        // write(connectionfd, temp_response, temp_response_len);

        // aesthetics
        printf("=====\n");
    }
}

/*
 *  parseQuery()
 *  Error checks and if the query is valid, it calls the appropriate
 *  operator for the query
 */
void parseQuery(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
        raiseError(connectionfd, "parseQuery\0", "Query was NULL.\0", NULL);

    // if quitting
    if (strcmp(query, "Quit\0") == 0)
    {
        printf("Goodbye.\n");
        quit(connectionfd);
    }

    // check for keyword "create"
    if (strstr(query, "create") != NULL)
        create(connectionfd, query);
}

/*
 *  raiseError()
 *  Is called when incorrect behavior occurs
 *  Note: exception_info is an optional argument, NULL if not needed
 */
void raiseError(int connectionfd, char* function, char* exception, char* exception_info)
{
    // print exception, close client, and quit
    if ((exception == NULL) && (exception_info == NULL))
        printf("Both arguments passed to raiseError() were NULL.\n");
    else if (exception_info == NULL)
        printf("Exception raised in function %s(): %s.\n", function, exception);
    else
    {
        int tilda_index = -1;
        for (int i = 0, len = strlen(exception); i < len; i++)
        {
            if (exception[i] == '~')
            {
                tilda_index = i;
                break;
            }
        }
        if (tilda_index == -1)
            printf("No tilda exists in the exception string, format is therefore incorrect.\n");
        else
        {
            // replace the tilda with the extra info
            for (int i = 0; i < tilda_index; i++)
                printf("%c", exception[i]);
            printf("`%s`", exception_info);
            for (int i = tilda_index, len = strlen(exception); i < len; i++)
            {
                if (i != tilda_index)
                    printf("%c", exception[i]);
            }
            printf(".\n");
        }
    }

    // exit gracefully
    quit(connectionfd);
}

/*
 *  quit()
 *  Is called anytime the program is correctly quitting
 */
void quit(int connectionfd)
{
    close(connectionfd);
    printf("=====\n");
    exit(1);
}

/*
 *  create()
 *  Is used to create a column (represented as a binary file on disk) in the 
 *  database
 */
void create(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
        raiseError(connectionfd, "create\0", "Query was NULL\0", NULL);

    // parse the query
    char* last;
    strtok_r(query, "(", &last);
    char* column = strtok_r(NULL, ",", &last);
    char* storage = strtok_r(NULL, ")", &last);
    if (column == NULL || storage == NULL)
        raiseError(connectionfd, "create\0", "The specified column or storage was NULL\0", NULL);
    else if (!((strcmp(storage, "\"unsorted\"") == 0) || (strcmp(storage, "\"sorted\"") == 0) || (strcmp(storage, "\"b+tree\"") == 0)))
        raiseError(connectionfd, "create\0", "The specified storage does not match unsorted, sorted, or b+tree\0", NULL);

    // capture the storage type
    int storage_id = (strcmp(storage, "\"unsorted\"") == 0) ? UNSORTED : 
                       ((strcmp(storage, "\"sorted\"") == 0)   ? SORTED  :
                       ((strcmp(storage, "\"b+tree\"") == 0)   ? BTREE   : -1));
    if ((storage_id != UNSORTED) && (storage_id != SORTED) && (storage_id != BTREE))
        raiseError(connectionfd, "create\0", "The specified storage does not match the storage id for either unsorted, sorted, or b+tree\0", NULL);

    // create a file with the appropriate header
    char* path = malloc(4 + strlen(column));
    path[0] = 'd'; path[1] = 'b'; path[2] = '/';
    strncat(path, column, strlen(column));
    if (path == NULL)
        raiseError(connectionfd, "create\0", "The path for the column ~ was NULL\0", column);
    FILE* fp = fopen(path, "wb");
    if (fp == NULL)
        raiseError(connectionfd, "create\0", "The filepointer created for the path ~ was NULL\0", path);
    fwrite(&storage_id, sizeof(int), 1, fp);
    fclose(fp);

    // create a message for the client
    // the +1s are so that it terminates each string
    char* prefix = "Created column `\0";
    char* suffix = "` in the database.\0";
    char* message = malloc(strlen(prefix) + strlen(suffix) + strlen(column) + 1);
    strncpy(message, prefix, strlen(prefix) + 1);
    strncpy(message + strlen(prefix), column, strlen(column) + 1);
    strncpy(message + strlen(prefix) + strlen(column), suffix, strlen(suffix) + 1);

    // variables for socket transfer
    int message_len = strlen(message) + 1;
    int storage_c_bytes = 0;
    bool storage_bool = 1;

    // write the message to the client
    write(connectionfd, &message_len, sizeof(int));
    while ((storage_c_bytes = recv(connectionfd, &storage_bool, sizeof(bool), 0)) <= 0);
    write(connectionfd, message, message_len);

    // print the message in the server and cleanup
    printf("%s\n", message);
    free(message);
}

/*
 *  createDatabaseDirectoryIfNotPresent()
 *  Creates the directory that will store all of the database files if it doesn't yet exist
 */
void createDatabaseDirectoryIfNotPresent(void)
{
    // create a test filepointer
    FILE* fp = fopen("db/", "r");
    if (fp == NULL)
    {
        system("mkdir db");
        printf("Created the directory `db/` for storing database files.\n");
    }
    fclose(fp);
}













