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

// function prototypes
void evaluate_commands(int connectionfd);

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
    evaluate_commands(connectionfd);
}

/*
 *  evaluate_commands()
 *  Receives queries from the client and evaluates them
 */
 void evaluate_commands(int connectionfd)
 {
    
 }




