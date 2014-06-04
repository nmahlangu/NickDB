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

// defined constants
#define QUERY_TYPES 21             // number of supported queries (e.g. select, fetch, update)
#define QUERY_ARGUMENTS 15         // maximum number of arguments for a query

// variables
int socketfd;                      // socket file descriptor for the server
char* queries[QUERY_TYPES];        // array of supported queries

// function prototypes
void get_query(void);

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
       return 1;
    }

    // print message
    printf("Connection received from file descriptor %d.\n", socketfd);
    printf("Ready to send queries to server.\n");
    printf("=====\n");

    // define the supported queries
    queries[0] = "select";    queries[1] = "fetch";     queries[2] = "create";    queries[3] = "load";
    queries[4] = "insert";    queries[5] = "tuple";     queries[6] = "min";       queries[7] = "max";
    queries[8] = "sum";       queries[9] = "avg";       queries[10] = "add";      queries[11] = "sub";
    queries[12] = "mul";      queries[13] = "div";      queries[14] = "print";    queries[15] = "hashjoin";
    queries[16] = "sortjoin"; queries[17] = "loopjoin"; queries[18] = "treejoin"; queries[19] = "delete";
    queries[20] = "update";

    // get queries
    while(1)
    {
        get_query();
        usleep(20000);
    }
}

/*
 *  get_query()
 *  Gets queries from the command line
 */
 void get_query(void)
 {
    // get a string from the command line 
    // determine if the user is typing or piping 
    char* query = malloc(BUFSIZ * sizeof(char));
    if (!isatty(fileno(stdin)))
        printf("Query: ");
    query = readline("Query: ");
    if (!isatty(fileno(stdin)) && query != NULL)
        printf("%s\n", query);


 }






















