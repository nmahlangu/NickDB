#include <stdio.h> 
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h> 
#include <unistd.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <stdbool.h>

// defined constants
#define QUERY_TYPES 21             // number of supported queries (e.g. select, fetch, update)
#define QUERY_ARGUMENTS 15         // maximum number of arguments for a query

// variables
int socketfd;                      // socket file descriptor for the server
char* queries[QUERY_TYPES];        // array of supported queries

// function prototypes
void getQuery(void);
void parseQuery(char* query);

// error handling
void quit(void);

int main(int argc, char const *argv[])
{
    // set up a socket and get the file descriptor of the server
    socketfd = 0; 
    char recvBuff[1024];
    struct sockaddr_in serv_addr; 
    char* server_ip = "127.0.0.1";
    memset(recvBuff, '0', sizeof(recvBuff));
    if ((socketfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("Error: Count not create socket\n");
        return 1;
    } 
    memset(&serv_addr, '0', sizeof(serv_addr)); 
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(5000);

    // clear terminal window (for aesthetics)
    printf("\033[2J");
    printf("\033[%d;%dH", 0, 0);

    // welcome message
    printf("=============================== NickDB - Client ===============================\n");
    printf("Waiting for a server to connect to....\n");

    // connect to the client
    if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr)<=0)
    {
        printf("inet_pton error occured\n");
        return 1;
    }
    if (connect(socketfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
       printf("The call to socket() failed, please wait a few seconds and try again.\n");
       return 2;
    }

    // print message
    printf("Connection received from file descriptor %d.\n", socketfd);
    printf("Ready to send queries to server.\n");
    printf("=====\n");

    // define the supported queries
    queries[0] = "select\0";    queries[1] = "fetch\0";     queries[2] = "create\0";    queries[3] = "load\0";
    queries[4] = "insert\0";    queries[5] = "tuple\0";     queries[6] = "min\0";       queries[7] = "max\0";
    queries[8] = "sum\0";       queries[9] = "avg\0";       queries[10] = "add\0";      queries[11] = "sub\0";
    queries[12] = "mul\0";      queries[13] = "div\0";      queries[14] = "print\0";    queries[15] = "hashjoin\0";
    queries[16] = "sortjoin\0"; queries[17] = "loopjoin\0"; queries[18] = "treejoin\0"; queries[19] = "delete\0";
    queries[20] = "update\0";

    // get queries
    while(1)
    {
        getQuery();
        usleep(20000);
    }
}

/*
 *  getQuery()
 *  Gets queries from the command line
 */
void getQuery(void)
{
    // get a string from the command line 
    // determine if the user is typing or piping 
    char* query = malloc(BUFSIZ * sizeof(char));
    if (!isatty(fileno(stdin)))
        printf("Query: ");
    query = readline("Query: ");
    if (!isatty(fileno(stdin)) && query != NULL)
        printf("%s\n", query);

    // variables
    int query_len = strlen(query);
    query[query_len++] = '\0';
    bool storage;
    int storage_a_bytes = 0;

    // write the query length then the query
    write(socketfd, &query_len, sizeof(int));          
    while ((storage_a_bytes = recv(socketfd, &storage, sizeof(bool), 0)) <= 0); 
    write(socketfd, query, query_len * sizeof(char));      

    // check if quitting  
    if (strcmp(query, "Quit\0") == 0)
    {
        printf("Goodbye.\n");
        quit();        
    }

    // get the number of chars in the response message
    int message_length;
    int storage_b_bytes = 0;
    while ((storage_b_bytes = recv(socketfd, &message_length, sizeof(int), 0)) <= 0);    
    write(socketfd, &storage, sizeof(bool));          

    // get the response message and print it
    char* response = malloc(message_length * sizeof(char));
    int storage_c_bytes = 0;
    while ((storage_c_bytes = recv(socketfd, response, message_length, 0)) <= 0);       
    printf("%s\n", response);

    // ******* DO NOT DELETE | USED TO TEST FOR NULL TERMINATOR ******** //
    // for (int i = 0; i < message_length; i++)
    //     printf("[%c]", response[i]);  
    // fflush(stdout);

    // clean up and aesthetics
    free(response);
    free(query);
    printf("=====\n");                        
}

/*
 *  quit()
 *  Is called anytime the program is correctly quitting
 */
void quit(void)
{
    printf("=====\n");
    exit(1);
}





















