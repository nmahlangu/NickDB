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
void writeResponseToClient(int connectionfd, char* response);
char* createCustomMessage(int connectionfd, char* prefix, char* stringToBeInserted, char* suffix);
void createOperator(int connectionfd, char* query);
void selectOperator(int connectionfd, char* query);
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
    bool storage = 1;

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
    {
        raiseError(connectionfd, "parseQuery\0", "Query was NULL\0", NULL);
        return;
    }

    // if quitting
    else if ((strcmp(query, "Quit\0") == 0) || (strcmp(query, "quit\0") == 0))
    {
        printf("Goodbye.\n");
        quit(connectionfd);
    }

    // check for keyword "create"
    else if (strstr(query, "create") != NULL)
    {
        createOperator(connectionfd, query);
    }

    // check for keyword "select"
    else if (strstr(query, "select") != NULL)
    {
        selectOperator(connectionfd, query);
    }

    // if not a valid command
    else
    {
        raiseError(connectionfd, "parseQuery\0", "Query was not a valid command\0", NULL);
        return;
    }
}

/*
 *  raiseError()
 *  Is called when incorrect behavior occurs
 *  Note: exception_info is an optional argument, NULL if not needed
 */
void raiseError(int connectionfd, char* function, char* exception, char* exception_info)
{
    // error checking
    if ((exception == NULL) && (exception_info == NULL))
    {
        printf("Both arguments passed to raiseError() were NULL.\n");
    }
    // if not supplementary information was provided
    else if (exception_info == NULL)
    {
        printf("Exception raised in function %s(): %s.\n", function, exception);
    }
    // if supplementary information was given
    else
    {
        // determine where to insert the supplementary information
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
        // insert the supplementary information in place of the tilda
        else
        {
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

    // tell the client the query was not evaluated
    char* errorMessage = "Query was not evaluated, see the server for an error log.\0";
    writeResponseToClient(connectionfd, errorMessage);

    // exit gracefully
    // quit(connectionfd);
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
 *  writeResponseToClient()
 *  Is used to write a response to the client after a query has executed.
 *  Note: response must be NULL terminated
 */
void writeResponseToClient(int connectionfd, char* response)
{
    // error checking
    if (response == NULL)
    {
        raiseError(connectionfd, "writeResponseToClient\0", "response to write to the client was NULL\0", NULL);
        return;
    }

    // write the response to the client
    int responseLength = strlen(response) + 1;
    int bytesReceivedFromClient = 0;
    bool storageBool;
    write(connectionfd, &responseLength, sizeof(int));
    while ((bytesReceivedFromClient = recv(connectionfd, &storageBool, sizeof(bool), 0)) <= 0);
    write(connectionfd, response, responseLength);
}

/*
 *  createCustomMessage()
 *  Returns a custom, concatenated string from 3 substrings
 */
 char* createCustomMessage(int connectionfd, char* prefix, char* stringToBeInserted, char* suffix)
 {
    // error checking
    if (prefix == NULL)
    {
        raiseError(connectionfd, "createCustomMessage\0", "prefix was NULL\0", NULL);
        return NULL;
    }
    else if (stringToBeInserted == NULL)
    {
        raiseError(connectionfd, "createCustomMessage\0", "stringToBeInserted was NULL\0", NULL);
        return NULL;
    }
    else if (suffix == NULL)
    {
        raiseError(connectionfd, "createCustomMessage\0", "suffix was NULL\0", NULL);
        return NULL;
    }

    // create and return the message
    char* space = " \0";
    char* message = malloc(strlen(prefix) + strlen(stringToBeInserted) + strlen(suffix) + 5);
    strncpy(message, prefix, strlen(prefix) + 1);
    strncpy(message + strlen(prefix), space, strlen(space) + 1);
    strncpy(message + strlen(prefix) + strlen(space), stringToBeInserted, strlen(stringToBeInserted) + 1);
    strncpy(message + strlen(prefix) + strlen(space) + strlen(stringToBeInserted), space, strlen(space) + 1);
    strncpy(message + strlen(prefix) + (2 * strlen(space)) + strlen(stringToBeInserted), suffix, strlen(suffix) + 1);
    return message;
 }

/*
 *  create()
 *  Is used to create a column (represented as a binary file on disk) in the 
 *  database
 */
void createOperator(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
    {
        raiseError(connectionfd, "createOperator\0", "Query was NULL\0", NULL);
        return;
    }

    // parse the query
    char* last;
    strtok_r(query, "(", &last);
    char* column = strtok_r(NULL, ",", &last);
    char* storage = strtok_r(NULL, ")", &last);
    if (column == NULL || storage == NULL)
    {
        raiseError(connectionfd, "createOperator\0", "The specified column or storage was NULL\0", NULL);
        return;
    }
    else if (!((strcmp(storage, "\"unsorted\"") == 0) || (strcmp(storage, "\"sorted\"") == 0) || (strcmp(storage, "\"b+tree\"") == 0)))
    {
        raiseError(connectionfd, "createOperator\0", "The specified storage does not match unsorted, sorted, or b+tree\0", NULL);
        return;
    }

    // capture the storage type
    int storage_id = (strcmp(storage, "\"unsorted\"") == 0) ? UNSORTED : 
                       ((strcmp(storage, "\"sorted\"") == 0)   ? SORTED  :
                       ((strcmp(storage, "\"b+tree\"") == 0)   ? BTREE   : -1));
    if ((storage_id != UNSORTED) && (storage_id != SORTED) && (storage_id != BTREE))
    {
        raiseError(connectionfd, "createOperator\0", "The specified storage does not match the storage id for either unsorted, sorted, or b+tree\0", NULL);
        return;
    }

    // create a file with the appropriate header
    char* path = malloc(4 + strlen(column));
    path[0] = 'd'; path[1] = 'b'; path[2] = '/';
    strncat(path, column, strlen(column));
    if (path == NULL)
    {
        raiseError(connectionfd, "createOperator\0", "The path for the column ~ was NULL\0", column);
        free(path);
        return;
    }
    FILE* fp = fopen(path, "wb");
    if (fp == NULL)
    {
        raiseError(connectionfd, "createOperator\0", "The filepointer created for the path ~ was NULL\0", path);
        free(path);
        return;
    }
    fwrite(&storage_id, sizeof(int), 1, fp);
    fclose(fp);

    // create a message and write it to the client
    // the +1s are so that it terminates each string
    char* prefix = "Created column `\0";
    char* suffix = "` in the database.\0";
    char* message = malloc(strlen(prefix) + strlen(suffix) + strlen(column) + 1);
    strncpy(message, prefix, strlen(prefix) + 1);
    strncpy(message + strlen(prefix), column, strlen(column) + 1);
    strncpy(message + strlen(prefix) + strlen(column), suffix, strlen(suffix) + 1);
    writeResponseToClient(connectionfd, message);

    // print the message in the server and cleanup
    printf("%s\n", message);
    free(message);
}

/*
 *  select()
 *  Is used to return the positions of matching data in the query. The result is either
 *  stored in a variable or returned to the user right away. 
 */
void selectOperator(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
    {
        raiseError(connectionfd, "selectOperator\0", "Query was NULL\0", NULL);
        return;
    }

    // parse the query
    char* last;
    strtok_r(query, "(", &last);
    char* firstArgument = strtok_r(NULL, ",)", &last);
    //char* secondArgument = strtok_r(NULL, ",)", &last);
    //char* thirdArgument = strtok_r(NULL, ",)", &last);
    if (firstArgument == NULL)
    {
        raiseError(connectionfd, "selectOperator\0", "firstArgument of the query was NULL\0", NULL);
        return;
    }

    // open column and see if it's valid
    char* column = malloc(strlen(firstArgument) + 3);
    sprintf(column, "db/%s",firstArgument);
    FILE* fp = fopen(column, "rb");
    if (fp == NULL)
    {
        raiseError(connectionfd, "selectOperator\0", "Unable to do this selection. The column ~ does not exist in the database", firstArgument);
        free(column);
        return;
    }

    // read the header to see what kind of storage the file has
    int headerInteger;
    fread(&headerInteger, sizeof(int), 1, fp);
    if ((headerInteger != UNSORTED) && (headerInteger != SORTED) && (headerInteger != BTREE))
    {
        raiseError(connectionfd, "selectOperator\0", "Unable to do this selection. The column ~ does not have valid header info", NULL);
        free(column);
        fclose(fp);
        return;
    }
    char* mess = createCustomMessage(connectionfd, "Test message: Unable to insert", firstArgument, "into the database");
    printf("[%s]\n", mess);
}

















