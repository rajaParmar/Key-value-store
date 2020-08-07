#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <iostream>
#include "constants.cpp"
#include "cxml.cpp"

using namespace std;

char buffer[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE];

bool send_all(int socket, void *buffer, size_t length)
{
    char *ptr = (char*) buffer;
    while (length > 0)
    {
        int i = send(socket, ptr, length,0);
        if (i < 1) return false;
        ptr += i;
        length -= i;
    }
    return true;
}
bool  recv_all(int socket,void *buffer, size_t length){
    size_t len = MAX_BUFFER_SIZE;
    char *pt = (char *)buffer;
    ssize_t n;
    while ( len > 0 && (n=recv(socket,pt,len,0)) > 0 ) {
        pt += n;
        len =- (size_t)n;
    }
    return true;

}
//============================================================================================

int main(int argc, char const *argv[]) {
    //int a;
    char input[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE];
    int server_connection_socket;
    struct sockaddr_in saddr;
    saddr.sin_family = AF_INET;
    saddr.sin_addr.s_addr = inet_addr(argv[1]);
    saddr.sin_port = htons(atoi(argv[2]));
    server_connection_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (server_connection_socket == -1) {
        perror("Network Error:Could not create socket\n");
        exit(-1);
    }

    if (connect(server_connection_socket, (struct sockaddr *) &saddr, sizeof(saddr)) == -1) {
        perror("Network Error:Could not connect\n");
        exit(-1);
    }

    if(argc == 2) {
        freopen(argv[1], "r", stdin);
        freopen("batchResponses.txt", "w", stdout);
        flag = 1;
    } else {
        flag = 0;
        user_format();
    }
    //printf("clienrt=============fv");
    while (1) {
        memset(buffer, 0, strlen(buffer));
        memset(input, 0, strlen(input));
        scanf("%s", input);
        if(input[0] == '\0') {
            break;
        }
        //printf("input=%s",input);
        const char *cxml = convert_raw_to_xml(input);
        if (strcmp(cxml, "NULL") == 0) {
            continue;
        }
        //printf("cxml=%d",(int)strlen(cxml));
        strcpy(buffer, cxml);

        if (send(server_connection_socket, buffer, strlen(buffer), 0) < 0) {
            perror("Network Error:Could not send data");
        }
//        if(!send_all(server_connection_socket,buffer,strlen(buffer)))
//            printf("false");
//        printf("send len=%d\n",(int) strlen(buffer));
//        memset(buffer, 0, strlen(buffer));

        if (read(server_connection_socket, buffer, sizeof(buffer)) < 0) {
            perror("Network Error:Could not receive data");
        }
//        if(!recv_all(server_connection_socket,buffer,MAX_BUFFER_SIZE))
//            printf("false");
//        //convert_xml_to_raw(buffer);
        //printf("recved==========%d\n\n",(int) strlen(buffer));
        convert_xml_to_raw(buffer);
        memset(input, 0, strlen(input));
    }
    return 0;
}