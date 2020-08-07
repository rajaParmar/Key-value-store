#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <string>
#include <regex>
#include "constants.cpp"

using namespace std;

struct args {
    const char *command;
    char *key;
    char *value;
    const char *result_value;
    const char *response;
};

bool checkIfCommandIsJoin(const char *xml) {
    string s(xml);
    regex b("type=\"join\">");
    smatch m;
    return regex_search(s, m, b);
}

bool checkIfCommandIsShareKey(const char *xml) {
    string s(xml);
    regex b("type=\"share_key\">");
    smatch m;
    return regex_search(s, m, b);
}

bool checkIfCommandIsServerList(const char *xml) {
    string s(xml);
    regex b("type=\"peerlist\">");
    smatch m;
    return regex_search(s, m, b);
}

bool checkIfCommandIsWelcomeNewServer(const char *xml) {
    string s(xml);
    regex b("type=\"welcome_new_server\">");
    smatch m;
    return regex_search(s, m, b);
}

void getCommandFromXML(char *xml, char *buffer) {
    string s(xml);
    regex b("<KVMessage type=\"[a-zA-Z]+\">");
    smatch m;
    regex_search(s, m, b);
    /*regex ip("[.0-9]+{3}");
    //smatch m1;
    string s1(m[0]);
    regex_search(s1, m, ip);
    //cout << m[0] << " ";
    //cout << x << " ";
    string s2(m[0]);
    strcpy(buffer, s2.c_str());*/
}

void getIPFromXML(char *xml, char *buffer) {
    string s(xml);
    regex b("<Ip>[.0-9]+{3}</Ip>");
    smatch m;
    regex_search(s, m, b);
    //auto x = m[0];
    regex ip("[.0-9]+{3}");
    //smatch m1;
    string s1(m[0]);
    regex_search(s1, m, ip);
    //cout << m[0] << " ";
    //cout << x << " ";
    string s2(m[0]);
    strcpy(buffer, s2.c_str());
}

void getPortFromXML(char *xml, char *buffer) {
    string s(xml);
    regex b("<Port>[0-9]+</Port>");
    smatch m;
    regex_search(s, m, b);
    //auto x = m[0];
    regex port("[0-9]+");
    //smatch m1;
    string s1(m[0]);
    regex_search(s1, m, port);
    //cout << m[0] << " ";
    //cout << x << " ";
    string s2(m[0]);
    strcpy(buffer, s2.c_str());
}

void getServerListFromXML(char *xml, char *buffer){
    string s(xml);
    regex b("<List>[0-9,@.]+</List>");
    smatch m;
    regex_search(s, m, b);
    //auto x = m[0];
    regex port("[0-9,@.]+");
    //smatch m1;
    string s1(m[0]);
    regex_search(s1, m, port);
    //cout << m[0] << " ";
    //cout << x << " ";
    string s2(m[0]);
    memset(buffer, 0, sizeof(buffer));
    strcpy(buffer, s2.c_str());
}

void getWelcomeNewServerFromXML(char *xml, char *buffer){
    string s(xml);
    regex b("<detail>[0-9,@.]+</detail>");
    smatch m;
    regex_search(s, m, b);
    //auto x = m[0];
    regex port("[0-9,@.]+");
    //smatch m1;
    string s1(m[0]);
    regex_search(s1, m, port);
    //cout << m[0] << " ";
    //cout << x << " ";
    string s2(m[0]);
    memset(buffer, 0, sizeof(buffer));
    strcpy(buffer, s2.c_str());
}

void createServerListXML(const char *list, char *buffer) {
    char p1[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                               "<KVMessage type=\"peerlist\">\n"
                                               "<List>";
    strcpy(buffer, p1);
    strcat(buffer, list);

    char p3[GEN_BUFFER_SIZE] = "</List>\n"
                               "</KVMessage>\0";
    strcat(buffer, p3);

}

void createWelcomeNewServerXML(const char *ip_port,char *buffer){
    char p1[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                               "<KVMessage type=\"welcome_new_server\">\n"
                                               "<detail>";
    strcpy(buffer, p1);
    strcat(buffer, ip_port);

    char p3[GEN_BUFFER_SIZE] = "</detail>\n"
                               "</KVMessage>\0";
    strcat(buffer, p3);
}

void  *create_join_req(char *ip, const char *port){
    char p1[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                               "<KVMessage type=\"join\">\n"
                                               "<Ip>";
    strcat(p1, ip);

    char p2[] = "</Ip>\n"
                "<Port>";
    strcat(p1, p2);
    strcat(p1, port);
    char p3[GEN_BUFFER_SIZE] = "</Port>\n"
                               "</KVMessage>\0";
    strcat(p1, p3);
    return strdup(p1);
}

void  *create_share_key_req(const char *ip, const char *port){
    char p1[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                               "<KVMessage type=\"share_key\">\n"
                                               "<Ip>";
    strcat(p1, ip);

    char p2[] = "</Ip>\n"
                "<Port>";
    strcat(p1, p2);
    strcat(p1, port);
    char p3[GEN_BUFFER_SIZE] = "</Port>\n"
                               "</KVMessage>\0";
    strcat(p1, p3);
    return strdup(p1);
}

void *create_put_req(const char *key, const char *value){
    char p1[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                               "<KVMessage type=\"putreq\">\n"
                                               "<key>";
    strcat(p1, key);

    char p2[] = "</key>\n"
                "<value>";
    strcat(p1, p2);
    strcat(p1, value);
    char p3[GEN_BUFFER_SIZE] = "</value>\n"
                               "</KVMessage>\0";
    strcat(p1, p3);
    return strdup(p1);
}

void *convert_xml_to_raw(char *xml, struct args *reqArgs) {
    char *token = strtok(xml, "\n");
    token = strtok(NULL, "\n");
    char *type = (token + 11);
    if (strcmp(type, "type=\"getreq\">") == 0) {
        reqArgs->command = "get";
        char *key = strtok(NULL, "\n");
        key += 5;
        int i = 0;
        while (*(key + i) != '<') {
            i++;
        }
        reqArgs->key = (char *) malloc(i + 1);
        strncpy(reqArgs->key, key, i);
        reqArgs->key[i] = '\0';
    } else if (strcmp(type, "type=\"putreq\">") == 0) {
        reqArgs->command = "put";
        char *key = strtok(NULL, "\n");
        key += 5;
        int i = 0;
        while (*(key + i) != '<') {
            i++;
        }
        reqArgs->key = (char *) malloc(i + 1);
        strncpy(reqArgs->key, key, i);
        reqArgs->key[i] = '\0';
        //puts("end of sxml");
        //Value
        char *value = strtok(NULL, "\n");
        value += 7;
        i = 0;
        while (*(value + i) != '<') {
            i++;
        }
        reqArgs->value = (char *) malloc(i + 1);
        strncpy(reqArgs->value, value, i);
        reqArgs->value[i] = '\0';

    } else if (strcmp(type, "type=\"delreq\">") == 0) {
        reqArgs->command = "del";
        char *key = strtok(NULL, "\n");
        key += 5;
        int i = 0;
        while (*(key + i) != '<') {
            i++;
        }
        reqArgs->key = (char *) malloc(i + 1);
        strncpy(reqArgs->key, key, i);
        reqArgs->key[i] = '\0';
    }    else if (strcmp(type, "type=\"join\">") == 0) {
        reqArgs->command = "join";
        char *ip = strtok(NULL, "\n");
        ip += 4;
        int i = 0;
        while (*(ip + i) != '<') {
            i++;
        }
        reqArgs->key = (char *) malloc(i + 1);
        strncpy(reqArgs->key, ip, i);
        reqArgs->key[i] = '\0';
        //puts("end of sxml");
        //Value
        char *port = strtok(NULL, "\n");
        port += 6;
        i = 0;
        while (*(port + i) != '<') {
            i++;
        }
        reqArgs->value = (char *) malloc(i + 1);
        strncpy(reqArgs->value, port, i);
        reqArgs->value[i] = '\0';

    }    else if (strcmp(type, "type=\"peerlist\">") == 0) {
        reqArgs->command = "peerlist";
        char *list = strtok(NULL, "\n");
        list += 6;
        int i = 0;
        while (*(list + i) != '<') {
            i++;
        }
        strcpy(reqArgs->key,"peerlist");
        reqArgs->value = (char *) malloc(i + 1);
        strncpy(reqArgs->value, list, i);
        reqArgs->value[i] = '\0';

    }

    return reqArgs;
}


char *GET_RES(const char *key, const char *value) {
    char p1[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                                               "<KVMessage type=\"resp\">\n"
                                                               "<Key>";
    strcat(p1, key);

    char p2[] = "</Key>\n"
                "<Value>";
    strcat(p1, p2);
    strcat(p1, value);
    char p3[GEN_BUFFER_SIZE] = "</Value>\n"
                "</KVMessage>\0";
    strcat(p1, p3);
    return strdup(p1);
}

char *PUT_RES() {
    char p1[GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                   "<KVMessage type=\"resp\">\n"
                                   "<Message>success</Message>\n"
                                   "</KVMessage>\0";
    return strdup(p1);
}

char *DEL_RES() {
    char p1[GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                               "<KVMessage type=\"resp\">\n"
                               "<Message>success</Message>\n"
                               "</KVMessage>\0";
    //strcat(p1, "success");
    //char p2[GEN_BUFFER_SIZE] = "</Message>\n"
    //            "</KVMessage>\0";
    //strcat(p1, p2);
    return strdup(p1);
}

char *ERROR_RES(const char *error) {
    char p1[GEN_BUFFER_SIZE + strlen(error)] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                               "<KVMessage type=\"resp\">\n"
                                               "<Message>error,";
    strcat(p1, error);
    char p2[] = "</Message>\n"
                "</KVMessage>\0";
    strcat(p1, p2);
    return strdup(p1);
}
