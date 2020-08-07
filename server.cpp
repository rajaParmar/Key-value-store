#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <errno.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <ctype.h>
#include <iostream>
#include <string>
#include <algorithm>
#include "constants.cpp"
//#include "server_xml.cpp"
#include "threadpool.cpp"

#define FINGER_TABLE_SIZE 16
#define MAX_CLIENT 50
using namespace std;

//hash <string> hasher;
struct ip_port {
    char ip[20];
    int port;
    int hash;
};
struct ip_port *fingerTable;
int self_Hash;
string serversList ;

//pass message is responsible of sending a hash of the sourceip and port of new node to all the currently present ring nodes.
//if the receiving nodes have hash withing their range they will include them in their finger tables else discard!
void passMessage(int socket, char *messsage) {
    if (send(socket, messsage, strlen(messsage), 0) < 0) {
        perror("Network Error:Could not send data");
    }

}

void getMessage(int socket, char *buffer) {
    if (read(socket, buffer, sizeof(buffer)) < 0) {
        perror("Network Error:Could not receive data");
    }
}

//this function is responsible for sending the request to the correct server by sending sequential requests along the ring.
void continueLookup() {

}

int getPort(int main_socket) {
    struct sockaddr_in sin;
    socklen_t len = sizeof(sin);
    if (getsockname(main_socket, (struct sockaddr *) &sin, &len) == -1) {
        perror("getsockname");
        return -1;
    } else {
        return ntohs(sin.sin_port);
    }
}

int get_hash_value(const char *ip, const char *port) {
    char combo[30];
    strcpy(combo, ip);
    strcat(combo, "@");
    strcat(combo, port);
    return hasher(combo) % (1 << 16);
}

int get_hash_value(const char *combo) {
    return hasher(combo) % (1 << 16);
}

int create_socket(const char *ip, int port) {
    int connection_socket;
    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(ip);
    address.sin_port = htons(port);
    connection_socket = socket(AF_INET, SOCK_STREAM, 0);

    if (connection_socket == -1) {
        perror("Network Error:Could not create socket\n");
        exit(-1);
    }
    if (connect(connection_socket, (struct sockaddr *) &address, sizeof(address)) == -1) {
        perror("Network Error:Could not connect\n");
        exit(-1);
    }
    return connection_socket;
}

void close_socket(int socket) {
    shutdown(socket, 2);
    close(socket);
}

void printFingerTable() {
    for (int i = 0; i < FINGER_TABLE_SIZE; i++) {
        printf("ip address = %s port number = %d hash = %d\n", fingerTable[i].ip, fingerTable[i].port,
               fingerTable[i].hash);
    }
}

//to be called by individual servers and update the finger table accordingly to the received hash of the new server.
void updateFingerTable(struct ip_port *newnode) {
    int hash = get_hash_value(newnode->ip, to_string(newnode->port).c_str());
    int res;
    if (self_Hash > hash) {
        hash += (1 << 16);
    }

    for (int i = 0; i < 16; i++) {
        res = (self_Hash + (1 << i)) % (1 << 16);
        if ((fingerTable[i].hash > hash && hash >= res) || (fingerTable[i].hash<res && fingerTable[i].hash>(hash % 1 << 16))) {
            fingerTable[i].hash = hash % (1 << 16);
            strcpy(fingerTable[i].ip, newnode->ip);
            fingerTable[i].port = newnode->port;
        }
    }
}

//to be called by individual servers when it receives the successor from the main server(ie after join protocol has finished)
void createFingerTable(string serverList) {
    //cout << serverList << endl;
    fingerTable = (struct ip_port *) calloc(FINGER_TABLE_SIZE, sizeof(ip_port));
    int i = 0, hash;
    map<int, string> m;
    vector<int> v;

    stringstream check1(serverList);
    string token;
    cout << "self_hash = " << self_Hash << endl;
    while (getline(check1, token, ',')) {
        cout << token << " ";
        hash = get_hash_value(token.c_str());
        cout << hash << endl;
        if (self_Hash > hash) {
            hash += (1 << 16);
        }
        m[hash] = token;
        v.push_back(hash);
    }

    sort(v.begin(), v.end());
    for (int i = 0; i < 16; i++) {
        int res = (self_Hash + (1 << i)) % (1 << 16);
        //cout << "i = " << i << " res = " << res << " ";
        if (res <= v[v.size() - 1]) {
            vector<int>::iterator it = upper_bound(v.begin(), v.end(), res);
            hash = v[it - v.begin()];
        } else {
            hash = v[v.size() - 1];
        }

        //cout << "hash = " << hash << " ";
        fingerTable[i].hash = hash % (1 << 16);
        const char *ip = m[hash].c_str();
        //cout << m[hash] << endl;
        //puts(ip);
        int j;
        for (j = 0; ip[j] != '@'; j++);
        strncpy(fingerTable[i].ip, ip, j);
        fingerTable[i].port = atoi(ip + j + 1);
    }

    printFingerTable();
}

void broadcast_message(const char *serverList, char *message) {
    char *buffer = (char *) malloc(MAX_BUFFER_SIZE);
    createWelcomeNewServerXML(message, buffer);
    printf("welcome him= %s\n", buffer);
    stringstream check1(serverList);
    string token;
    getline(check1, token, ',');
    while (getline(check1, token, ',')) {
        stringstream check1(token);
        string ip, port;
        getline(check1, ip, '@');
        getline(check1, port, '@');
//        cout<< ip << endl;
//        cout<<port<<endl;
        int sock = create_socket(ip.c_str(), atoi(port.c_str()));
        passMessage(sock, buffer);
        close_socket(sock);
    }

}

void shareKeyValues(string keyValueList, char *newNodeip, char *newNodePort){
   // puts(keyValueList.c_str());
    stringstream check1(keyValueList);
    char *combo=(char *)malloc(20 * sizeof(char));
    char *buffer=(char *)malloc(MAX_BUFFER_SIZE);
    strcpy(combo, newNodeip);
    strcat(combo, "@");
    strcpy(combo, newNodePort);
    int kvHash = 0;
    int newNodeHash = get_hash_value(combo);
    int socket = create_socket(newNodeip, atoi(newNodePort));
    string token;
    string key, value;
    while (getline(check1, token, ',')) {
        kvHash = get_hash_value(token.c_str());
        if(kvHash <= newNodeHash){
            stringstream check1(token);
            getline(check1, key, '@');
            getline(check1, value, '@');
            strcpy(buffer, (char *)create_put_req(key.c_str() , value.c_str()));
            puts(buffer);
            printf("@@@@@@@@@@@@\n");
            passMessage(socket, buffer);
            memset(buffer, 0, sizeof(buffer));
            getMessage(socket,buffer);
            printf("receives===%s",buffer);
            memset(buffer, 0, sizeof(buffer));
            struct arg * temp=(struct arg*)malloc(sizeof(struct arg));
//            strcpy(temp->command,"DEL");
//            strcpy(temp->key, key.c_str());
//            strcpy(temp->value, value.c_str());
                temp->command= "DEL";
                temp->key= key.c_str();
                temp->value= value.c_str();
            DEL((void *)temp);
        }
    }
    close_socket(socket);
}

void getSuccessor(char *serverList, const char *ip, int port) {
    map<int, string> m;
    vector<int> v;

    self_Hash = get_hash_value(ip, to_string(port).c_str());
    cout << "self hash:" << self_Hash << endl;
    stringstream check1(serverList);
    string token;
    while (getline(check1, token, ',')) {
        //cout << token << "\n";
        int hash = get_hash_value(token.c_str());
        m[hash] = token;
        v.push_back(hash);
        //v.push_back(intermediate);
    }
    for (int x:v) {
        cout << x << ":" << m[x] << endl;
    }
    printf("searching\n");
    sort(v.begin(), v.end());
    int successor = -1;
    for (int x:v) {
        // cout<< x <<":" << m[x] <<endl;
        if (self_Hash < x) {
            successor = x;
            break;
        }
    }
    printf("foundid=%d ip=%s\n", successor, m[successor].c_str());
}

void sendShareKeyReq(char *ip, int port, int self_port){
    int socket = create_socket(ip,port);
    char *buffer = (char *) malloc(MAX_BUFFER_SIZE);

    const char *s_port = to_string(self_port).c_str();
    strcpy(buffer,(char *)create_share_key_req("127.0.0.1", s_port));
    //puts(buffer);
    passMessage(socket, buffer);
    close_socket(socket);
}

string get_data(){
    string final_string;
    string temp_key;
    string temp_value;
    const char *key_pattern = "<Key>";
    const char *key_end_pattern = "</Key>";
    const char *new_key_end_pattern = "</Key>\n";
    const char *val_end_pattern = "</Value>";
    const char *val_pattern = "<Value>";
    const char *tab_ekvpair = "\t</KVPair>\n";
    const char *temp_ekvpair = "</KVPair>";
    const char *file_name="Total";
    FILE *fp;
    fp = fopen(file_name, "r");
    char buffer[MAX_VALUE_SIZE + GEN_BUFFER_SIZE];

    while (fgets(buffer, MAX_VALUE_SIZE + GEN_BUFFER_SIZE, fp) != NULL) {
        string s = buffer;
        for (int i = 0; i < strlen(buffer); i++) {
            if (strcmp(s.substr(i, strlen(key_pattern)).c_str(), key_pattern) == 0) {
                int end = s.find(key_end_pattern);
                temp_key = s.substr(i + strlen(key_pattern), end - (strlen(key_pattern) + 2)).c_str();
                break;
            }
            if (strcmp(s.substr(i, strlen(val_pattern)).c_str(), val_pattern) == 0) {
                int end = s.find(val_end_pattern);
                temp_value = s.substr(i + strlen(val_pattern), end - (strlen(val_pattern) + 2)).c_str();
                break;
            }
            if (strcmp(s.substr(i, strlen(temp_ekvpair)).c_str(), temp_ekvpair) == 0) {
                //cout<<"key value pair found!";
                //cout<<temp_key<<" "<<temp_value<<endl;
                final_string.append(temp_key+"@"+temp_value+",");
            }
        }
    }
    return final_string;

}

int main(int argc, char **argv) {
    int main_socket, new_socket, addrlen, activity, getval, sd, max_sd, i = 0, options, count = 0, port = 0;
    char ip[12];
    int client_list[50];
    fd_set incomingfds;
    char *Buffer = (char *) malloc((MAX_BUFFER_SIZE));

    while (i < MAX_CLIENT) {
        client_list[i] = 0;
        i++;
    }
    main_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (main_socket == -1) {
        perror("Network Error:Could not create socket\n");
        exit(-1);
    }
    if (setsockopt(main_socket, SOL_SOCKET, SO_REUSEADDR, (char *) &options, sizeof(options)) < 0) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(MAIN_PORT);
    address.sin_addr.s_addr = inet_addr(INET_ADDR);

    if (bind(main_socket, (struct sockaddr *) &address, sizeof(address)) < 0) {
        address.sin_port = htons(0);
        if (bind(main_socket, (struct sockaddr *) &address, sizeof(address)) < 0) {
            perror("bind()");
            exit(1);
        }

        port = getPort(main_socket);
        char *ip = inet_ntoa(address.sin_addr);
        self_Hash = get_hash_value(ip, to_string(port).c_str());
        const char *portarr = to_string(port).c_str();

        strcpy(Buffer, (char *) create_join_req(ip, portarr));

    } else {
        serversList.append("127.0.0.1@"+ to_string(getPort(main_socket))+",");
        createFingerTable(serversList);

        //printFingerTable();
    }

//    intializing threadpool===================
    struct pool *p = create_pool(20);
    initialize_cache(SIZE_OF_SET, NUM_OF_SET);
    initiliase_store();

    if (listen(main_socket, 50) != 0) {
        perror("listen()");
        exit(1);
    }
    addrlen = sizeof(address);
    //printf("Waiting for connection...\n");
    printf("self port=%d\n",getPort(main_socket));

    if(getPort(main_socket) != MAIN_PORT ) {
        //printf("sending join req\n");
        int sock = create_socket(INET_ADDR, MAIN_PORT);
        passMessage(sock, Buffer);
    }
    while (1) {
        FD_ZERO(&incomingfds);
        FD_SET(main_socket, &incomingfds);
        max_sd = main_socket;
        for (i = 0; i < MAX_CLIENT; i++) {
            if (client_list[i] > 0) {
                FD_SET(client_list[i], &incomingfds);
            }
            if (client_list[i] > max_sd) {
                max_sd = client_list[i];
            }
        }
        activity = select(max_sd + 1, &incomingfds, NULL, NULL, NULL);
        if (activity < 0 && (errno != EINTR)) {
            printf("select error");
        }
        if (FD_ISSET(main_socket, &incomingfds)) {
            if ((new_socket = accept(main_socket, (struct sockaddr *) &address, (socklen_t * ) & addrlen)) < 0) {
                perror("accept");
                exit(EXIT_FAILURE);
            }

            for (i = 0; i < MAX_CLIENT; i++) {
                //if position is empty
                if (client_list[i] == 0) {
                    client_list[i] = new_socket;
                    //printf("Adding to list of sockets as %d\n", i);
                    count++;
                    break;
                }
            }
        }

        for (i = 0; i < MAX_CLIENT; i++) {
            sd = client_list[i];

            if (FD_ISSET(sd, &incomingfds)) {
                //Check if it was for closing, and also read the incoming msg
                memset(Buffer, 0, sizeof(Buffer));
                getval = read(sd, Buffer, (MAX_BUFFER_SIZE));
                //printf("getval = %d\n", getval);
                if (getval == 0) {
                    //Somebody disconnected , get his details and print
                    getpeername(sd, (struct sockaddr *) &address, (socklen_t * ) & addrlen);
                    //printf("Host disconnected , ip %s , port %d \n" ,inet_ntoa(address.sin_addr) , ntohs(address.sin_port));
                    count--;
                    //Close the socket and mark as 0 in list for reuse
                    close(sd);

                    client_list[i] = 0;
                } else {
                    //puts(Buffer);
                    if (checkIfCommandIsJoin(Buffer)) {
                        char *ip = (char *) malloc(20 * sizeof(char));
                        char *port = (char *) malloc(10 * sizeof(char));
                        getIPFromXML(Buffer, ip);
                        getPortFromXML(Buffer, port);

                        int node_id = get_hash_value(ip, port);
                        int socket = create_socket(ip, atoi(port));

                        memset(Buffer, 0, sizeof(Buffer));
                        createServerListXML(serversList.c_str(), Buffer);

                        passMessage(socket, Buffer);
                        memset(Buffer, 0, sizeof(Buffer));
                        close_socket(socket);
                        char *new_ip_port = (char *) malloc(25 * sizeof(char));
                        strcpy(new_ip_port, ip);
                        strcat(new_ip_port, "@");
                        strcat(new_ip_port, port);
                        broadcast_message(serversList.c_str(), new_ip_port);
                        serversList += ip;
                        serversList += "@";
                        serversList += port;
                        serversList += ",";

                        memset(Buffer, 0, sizeof(Buffer));
                        struct ip_port *newnode = (struct ip_port *) malloc(sizeof(struct ip_port));
                        newnode->hash = node_id;
                        strcpy(newnode->ip, ip);
                        newnode->port = atoi(port);
                        updateFingerTable(newnode);
                        printFingerTable();

                    } else if (checkIfCommandIsServerList(Buffer)) {
                        //memset(Buffer, 0, sizeof(Buffer));7
                        getServerListFromXML(Buffer, Buffer);
                        if (strcmp(Buffer, "") == 0) {
                            cout << "no ips received!  put main as successor";
                        } else {
                            //puts(Buffer);
                            //puts("Here");
                            int port = getPort(main_socket);

                            //printf("self port=%d\n", port);
                            //printf(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Creating finger table\n");
                            createFingerTable(Buffer);
                            //printf("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n");
                            memset(Buffer, 0, sizeof(Buffer));
                            sendShareKeyReq(fingerTable[0].ip,fingerTable[0].port, getPort(main_socket));
                            memset(Buffer, 0, sizeof(Buffer));
                        }

                    } else if (checkIfCommandIsWelcomeNewServer(Buffer)) {
                        //memset(Buffer, 0, sizeof(Buffer));7
                        getWelcomeNewServerFromXML(Buffer, Buffer);
                        if (strcmp(Buffer, "") == 0) {
                            cout << "no new node";
                        }

                        //printf("ip to update=%s\n", Buffer);
                        //updatefinger table here
                        struct ip_port *newnode = (struct ip_port *) malloc(sizeof(ip_port));
                        newnode->hash = get_hash_value(Buffer);
                        strcpy(newnode->ip, strtok(Buffer, "@"));
                        newnode->port = atoi(strtok(NULL, "@"));
                        //printf("ip= %s,  port= %d ,hash = %d \n",newnode->ip, newnode->port, newnode->hash);
                        updateFingerTable(newnode);
                        printFingerTable();

                    }  else if (checkIfCommandIsShareKey(Buffer)) {
                        char *ip = (char *) malloc(20 * sizeof(char));
                        char *port = (char *) malloc(10 * sizeof(char));
                       // puts("in share key");
                        //puts(Buffer);
                        getIPFromXML(Buffer, ip);
                        getPortFromXML(Buffer, port);
                        //puts(ip);
                        //puts(port);
                        string store = get_data();
                        //puts(store.c_str());
                        shareKeyValues(store, ip, port);

                        //int sock = create_socket(ip,atoi(port));
                        //send keys to this socket
                    }   else{
                        //puts(Buffer);
                        //printf("df##\n");
                        //memset(Buffer, 0, sizeof(Buffer));
                        //get,put,del
                        place_request(p, Buffer, sd);
                    }
                }
            }
        }
    }
    return 0;
}