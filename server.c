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

#include "intermediateResults.h"

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
void loadOperator(int connectionfd, char* query);
void createDatabaseDirectoryIfNotPresent(void);
bool increaseArraySizeByMultiplier(int connectionfd, int* array, int currentArraySize, int newArraySize);

// for error handling and quitting
void raiseDatabaseException(int connectionfd, char* function, char* exception, char* exception_info);
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
 *  Receives queries from the client and evaluates them.
 */
void evaluateCommands(int connectionfd)
{
    // variables
    char* query;
    int num_chars;
    int bytesReceivedFromClientA = 0;
    int bytesReceivedFromClientB = 0;
    bool storage = 1;

    // receive queries
    while(1)
    {
        // print message
        printf("Query: ");
        fflush(stdout);

        // get query and its length
        while ((bytesReceivedFromClientA = recv(connectionfd, &num_chars, sizeof(int), 0)) <= 0);
        query = malloc(num_chars * sizeof(int));
        write(connectionfd, &storage, sizeof(bool));
        while ((bytesReceivedFromClientB = recv(connectionfd, query, num_chars, 0)) <= 0);
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
 *  operator for the query.
 */
void parseQuery(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
    {
        raiseDatabaseException(connectionfd, "parseQuery\0", "Query was NULL\0", NULL);
        return;
    }

    // if quitting
    else if ((strcmp(query, "Quit\0") == 0) || (strcmp(query, "quit\0") == 0))
    {
        printf("Goodbye.\n");
        quit(connectionfd);
    }

    // check for keyword "create"
    else if (strstr(query, "create\0") != NULL)
    {
        createOperator(connectionfd, query);
    }

    // check for the keyword load
    else if (strstr(query, "load\0") != NULL)
    {
        loadOperator(connectionfd, query);
    }

    // check for keyword "select"
    else if (strstr(query, "select\0") != NULL)
    {
        selectOperator(connectionfd, query);
    }

    // if not a valid command
    else
    {
        raiseDatabaseException(connectionfd, "parseQuery\0", "Query was not a valid command\0", NULL);
        return;
    }
}

/*
 *  raiseDatabaseException()
 *  Is called when incorrect behavior occurs.
 *  Note: exception_info is an optional argument, NULL if not needed.
 */
void raiseDatabaseException(int connectionfd, char* function, char* exception, char* exception_info)
{
    // error checking
    if ((exception == NULL) && (exception_info == NULL))
    {
        printf("Both arguments passed to raiseDatabaseException() were NULL.\n");
    }
    // if no supplementary information was provided
    else if (exception_info == NULL)
    {
        printf("Database exception raised in function %s(): %s.\n", function, exception);
    }
    // if supplementary information was provided
    else
    {
        // determine where to insert the supplementary information
        int tildaIndex = -1;
        for (int i = 0, len = strlen(exception); i < len; i++)
        {
            if (exception[i] == '~')
            {
                tildaIndex = i;
                break;
            }
        }
        // make sure a tilda was placed in the exception string
        if (tildaIndex == -1)
        {
            printf("No tilda exists in the exception string, format is therefore incorrect.\n");
        }
        // insert the supplementary information in place of the tilda
        else
        {
            for (int i = 0; i < tildaIndex; i++)
            {
                printf("%c", exception[i]);
            }
            printf("`%s`", exception_info);
            for (int i = tildaIndex, len = strlen(exception); i < len; i++)
            {
                if (i != tildaIndex)
                    printf("%c", exception[i]);
            }
            printf(".\n");
        }
    }

    // tell the client the query was not evaluated
    char* errorMessage = "Query was not evaluated, see the server for an error log.\0";
    writeResponseToClient(connectionfd, errorMessage);
}

/*
 *  createDatabaseDirectoryIfNotPresent()
 *  Creates the directory that will store all of the database files if it doesn't yet exist.
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
 *  Is called anytime the program is correctly quitting.
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
 *  Note: response must be NULL terminated.
 */
void writeResponseToClient(int connectionfd, char* response)
{
    // error checking
    if (response == NULL)
    {
        raiseDatabaseException(connectionfd, "writeResponseToClient\0", "response to write to the client was NULL\0", NULL);
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
 *  Returns a custom, concatenated string from 3 substrings.
 */
 char* createCustomMessage(int connectionfd, char* prefix, char* stringToBeInserted, char* suffix)
 {
    // error checking
    if (prefix == NULL)
    {
        raiseDatabaseException(connectionfd, "createCustomMessage\0", "prefix was NULL\0", NULL);
        return NULL;
    }
    else if (stringToBeInserted == NULL)
    {
        raiseDatabaseException(connectionfd, "createCustomMessage\0", "stringToBeInserted was NULL\0", NULL);
        return NULL;
    }
    else if (suffix == NULL)
    {
        raiseDatabaseException(connectionfd, "createCustomMessage\0", "suffix was NULL\0", NULL);
        return NULL;
    }

    // create and return the concatenated string
    char* message = malloc(strlen(prefix) + strlen(stringToBeInserted) + strlen(suffix) + 3);
    strncpy(message, prefix, strlen(prefix) + 1);
    strncpy(message + strlen(prefix), stringToBeInserted, strlen(stringToBeInserted) + 1);
    strncpy(message + strlen(prefix) + strlen(stringToBeInserted), suffix, strlen(suffix) + 1);
    return message;
 }

 /*
  *  increaseArraySizeByMultiplier()
  *  Is used to increase an array's size on the heap.
  */
 bool increaseArraySizeByMultiplier(int connectionfd, int* array, int currentArraySize, int newArraySize)
 {
    // error checking
    if (array == NULL)
    {
        raiseDatabaseException(connectionfd, "increaseArraySizeByMultiplier\0", "array was NULL\0", NULL);
        return false;
    }

    // increase the size of the array
    array = realloc(array, newArraySize);
    if (array == NULL)
    {
        raiseDatabaseException(connectionfd, "increaseArraySizeByMultiplier\0", "The call to realloc returned a NULL pointer\0", NULL);
        return false;
    }

    // return that operation was successful
    return true;
 }

/*
 *  create()
 *  Is used to create a column (represented as a binary file on disk) in the 
 *  database.
 */
void createOperator(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
    {
        raiseDatabaseException(connectionfd, "createOperator\0", "Query was NULL\0", NULL);
        return;
    }

    // parse the query
    char* last;
    strtok_r(query, "(", &last);
    char* column = strtok_r(NULL, ",", &last);
    char* storage = strtok_r(NULL, ")", &last);
    if (column == NULL || storage == NULL)
    {
        raiseDatabaseException(connectionfd, "createOperator\0", "The specified column or storage was NULL\0", NULL);
        return;
    }
    else if (!((strcmp(storage, "\"unsorted\"") == 0) || (strcmp(storage, "\"sorted\"") == 0) || (strcmp(storage, "\"b+tree\"") == 0)))
    {
        raiseDatabaseException(connectionfd, "createOperator\0", "The specified storage does not match unsorted, sorted, or b+tree\0", NULL);
        return;
    }

    // capture the storage type
    int storageId = (strcmp(storage, "\"unsorted\"") == 0) ? UNSORTED : 
                       ((strcmp(storage, "\"sorted\"") == 0)   ? SORTED  :
                       ((strcmp(storage, "\"b+tree\"") == 0)   ? BTREE   : -1));
    if ((storageId != UNSORTED) && (storageId != SORTED) && (storageId != BTREE))
    {
        raiseDatabaseException(connectionfd, "createOperator\0", "The specified storage does not match the storage id for either unsorted, sorted, or b+tree\0", NULL);
        return;
    }

    // error check and create the file
    char* path = malloc(4 + strlen(column));
    sprintf(path, "db/%s", column);
    if (path == NULL)
    {
        raiseDatabaseException(connectionfd, "createOperator\0", "The path for the column ~ was NULL\0", column);
        free(path);
        return;
    }
    FILE* fp = fopen(path, "wb");
    if (fp == NULL)
    {
        raiseDatabaseException(connectionfd, "createOperator\0", "The filepointer created for the path ~ was NULL\0", path);
        free(path);
        return;
    }

    // write header data to the file. header consists of storage type and the number of bytes in the file
    int bytesInFile = 0;
    fwrite(&storageId, sizeof(int), 1, fp);
    fwrite(&bytesInFile, sizeof(int), 1, fp);
    fclose(fp);

    // create a message and write it to the client
    char* prefix = "Created column `\0";
    char* suffix = "` in the database.\0";
    char* message = createCustomMessage(connectionfd, prefix, column, suffix);
    if (message == NULL)
    {
        printf("Create operation was aborted due to above database exception.\n");
        free(path);
        return;
    }
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
        raiseDatabaseException(connectionfd, "selectOperator\0", "Query was NULL\0", NULL);
        return;
    }

    // make sure the user is storing the result
    if (strstr(query, "=") == NULL)
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "The result of a select must be stored in an intermediate variable\0", NULL);
        return;
    }

    // parse the query
    char* last;
    char* variableName = strtok_r(query, "=", &last);
    strtok_r(NULL, "(", &last);
    char* firstArgument = strtok_r(NULL, ",)", &last);
    char* secondArgument = strtok_r(NULL, ",)", &last);
    char* thirdArgument = strtok_r(NULL, ",)", &last);
    if (variableName == NULL)
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "variableName was NULL\0", NULL);
        return;
    }
    else if (firstArgument == NULL)
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "firstArgument of the query was NULL\0", NULL);
        return;
    }

    // make sure variable name is unique
    if (checkForIntermediateResultInLinkedList(variableName) != NULL)
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "The variable ~ already exists in memory, please rename the current intermediate result variable\0", variableName);
        return;
    }

    // open column and see if it's valid
    char* column = malloc(strlen(firstArgument) + 3);
    sprintf(column, "db/%s",firstArgument);
    FILE* fp = fopen(column, "rb");
    if (fp == NULL)
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "Unable to do this selection. The column ~ does not exist in the database\0", firstArgument);
        free(column);
        return;
    }

    // read the header to see what kind of storage the file has
    int headerStorageType;
    fread(&headerStorageType, sizeof(int), 1, fp);
    if ((headerStorageType != UNSORTED) && (headerStorageType != SORTED) && (headerStorageType != BTREE))
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "Unable to do this selection. The column ~ does not have valid header info\0", NULL);
        free(column);
        fclose(fp);
        return;
    }

    // read all data from the file into a buffer
    int headerStorageSize;
    fread(&headerStorageSize, sizeof(int), 1, fp);
    int* arrayOfFileData = malloc(headerStorageSize * sizeof(int));
    fread(arrayOfFileData, headerStorageSize, 1, fp);
    fclose(fp);

    // variables for position metadata
    int* validPositionsInArray = malloc(headerStorageSize * sizeof(int));
    int numberOfValidPositions = 0;

    // if selecting on the entire column
    if ((secondArgument == NULL) && (thirdArgument == NULL))
    {
        // add all positions as valid
        for (int i = 0; i < (headerStorageSize / sizeof(int)); i++)
            validPositionsInArray[numberOfValidPositions++] = i;
    }
    // if selecting the locations of just one value
    else if ((secondArgument != NULL) && (thirdArgument == NULL))
    {
        // add positions that have a value matching the entered number as valid
        int secondArgumentCastToInteger = atoi(secondArgument);
        for (int i = 0; i < (headerStorageSize / sizeof(int)); i++)
        {
            if (arrayOfFileData[i] == secondArgumentCastToInteger)
            {
                validPositionsInArray[numberOfValidPositions++] = i;
            }
        }
    }
    // if selecting between two values
    else if ((secondArgument != NULL) && (thirdArgument != NULL))
    {
        // add positions that are bound by the two values as valid
        int secondArgumentCastToInteger = atoi(secondArgument);
        int thirdArgumentCastToInteger = atoi(thirdArgument);
        for (int i = 0; i < (headerStorageSize / sizeof(int)); i++)
        {
            if (arrayOfFileData[i] >= secondArgumentCastToInteger
                && arrayOfFileData[i] <= thirdArgumentCastToInteger)
            {
                validPositionsInArray[numberOfValidPositions++] = i;
            }
        }
    }
    // error checking
    else
    {
        raiseDatabaseException(connectionfd, "selectOperator\0", "secondArgument was NULL and thirdArgument was not, which cannot happen in a valid query\0", NULL);
        return;
    }

    // store the result in an intermediate variable
    intermediateResult* variable = malloc(sizeof(intermediateResult));
    variable->variableName = malloc(strlen(variableName) + 1);
    strncpy(variable->variableName, variableName, strlen(variableName));
    variable->numberOfValidPositions = numberOfValidPositions;
    variable->validPositions = malloc(numberOfValidPositions * sizeof(int));
    memcpy((void*)variable->validPositions, (void*)validPositionsInArray, (numberOfValidPositions * sizeof(int)));
    variable->next = NULL;

    // store the intermediate variable
    insertIntermediateResultIntoLinkedList(variable);
    // printLinkedListOfIntermediateResults();   

    // create a message and write it to the client
    char* prefix = "Selected valid positions from the column `\0";
    char* suffix = "`.\0";
    char* message = createCustomMessage(connectionfd, prefix, firstArgument, suffix);
    if (message == NULL)
    {
        printf("Select operation was aborted due to above database exception.\n");
        free(arrayOfFileData);
        return;
    }
    writeResponseToClient(connectionfd, message);

    // print the message in the server and cleanup
    printf("%s\n", message);
    free(arrayOfFileData);
}

/*
 *  loadOperator()
 *  Is used to load .csv files into the database.
 */
void loadOperator(int connectionfd, char* query)
{
    // error checking
    if (query == NULL)
    {
        raiseDatabaseException(connectionfd, "loadOperator\0", "Query was NULL\0", NULL);
        return;
    }
    else if (strstr(query, "\"") == NULL)
    {
        raiseDatabaseException(connectionfd, "loadOperator\0", "Filename needs quotations around it\0", NULL);
        return;
    }

    // parse the query
    char* lasts;
    strtok_r(query, "\"", &lasts);
    char* fileName = strtok_r(NULL, "\"", &lasts);

    // create a path to the csvTables folder with that filename
    char* filePath = malloc(strlen(fileName) + 11);
    sprintf(filePath, "csvTables/%s", fileName);
    filePath[strlen(fileName) + 10] = '\0';

    // try to open the file
    FILE* fp = fopen(filePath, "r");
    if (fp == NULL)
    {
        raiseDatabaseException(connectionfd, "loadOperator\0", "The file ~ does not exist in the database\0", fileName);
        return;
    }

    // get all of the columns in the file
    char* columnBuffer = malloc(10 * BUFSIZ);
    int columnBufferIndex = 0;
    int numberOfColumns = 1;
    while(!feof(fp))
    {
        char character = fgetc(fp);
        if (character == '\n')
        {
            columnBuffer[columnBufferIndex++] = '\0';
            break;
        }
        else if (character == ',')
        {
            numberOfColumns++;
        }
        columnBuffer[columnBufferIndex++] = character;
    }

    // create an array of all the column names
    char** columnNames = malloc(numberOfColumns * sizeof(char*));
    char* position;
    for (int i = 0; i < numberOfColumns; i++)
    {
        // copy the string
        char* parsedColumnName = (i == 0) ? strtok_r(columnBuffer, ",", &position) : strtok_r(NULL, ",", &position);
        printf("parsedColumnName: [%s]\n", parsedColumnName);
        char* columnName = malloc(strlen(parsedColumnName) + 1);
        strncpy(columnName, parsedColumnName, strlen(parsedColumnName));
        columnNames[i] = columnName;

        // make sure the column exists in the database
        char* columnPath = malloc(strlen(columnName) + 4);
        sprintf(columnPath, "db/%s", columnName);
        FILE* columnFp = fopen(columnPath, "rb");
        if (columnFp == NULL)
        {
            raiseDatabaseException(connectionfd, "loadOperator\0", "Could not load file into database, create a database file for the column ~ first\0", columnName);
            free(columnNames);
            free(columnBuffer);
            return;
        }
        fclose(fp);
    }

    // create the an array for storing the integers
    int** columnData = malloc(numberOfColumns * sizeof(int*));
    for (int i = 0; i < numberOfColumns; i++)
    {
        columnData[i] = malloc(BUFSIZ * sizeof(int));
    }

    // read and store the integers
    int columnNumber = 0;
    int currentArrayIndex = 0;
    int currentArraySize = BUFSIZ * sizeof(int);
    int tempReadingVariable;
    while(1)
    {
        // read and store an integer | break if at the end of the file
        fscanf(fp, "%d", &tempReadingVariable);
        if (feof(fp))
        {
            break;
        }
        columnData[columnNumber][currentArrayIndex] = tempReadingVariable;

        // update counters
        if ((columnNumber + 1)== numberOfColumns)
        {
            currentArrayIndex++;
        }
        columnNumber = (columnNumber + 1) % numberOfColumns;

        // increase each array's size if necessary
        bool returnVal;
        if ((currentArrayIndex + 1) > currentArraySize)
        {
            int newSize = currentArraySize + (BUFSIZ * sizeof(int));
            for (int i = 0; i < numberOfColumns; i++)
            {
                returnVal = increaseArraySizeByMultiplier(connectionfd, columnData[i], currentArraySize, newSize);
                if (!returnVal)
                {
                    printf("Load operation was aborted due to the above exception\n");
                    for (int i = 0; i < numberOfColumns; i++)
                    {
                        free(columnData[i]);
                    }
                    free(columnData);
                    return;
                }
            }
            currentArraySize = newSize;
        }
    }

}
















