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

// function prototypes
void evaluateCommands(int connectionfd);

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
    int storage_c_bytes = 0;
    bool storage = 1;
    char* temp_response = "The query worked!\0";
    int temp_response_len = strlen(temp_response) + 1;

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

        // operators are called on the query
        // TODO

        // write a message back to the client
        write(connectionfd, &temp_response_len, sizeof(int));
        while ((storage_c_bytes = recv(connectionfd, &storage, sizeof(bool), 0)) <= 0);
        write(connectionfd, temp_response, temp_response_len);

        // aesthetics
        printf("=====\n");
    }
 }




