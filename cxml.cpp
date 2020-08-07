#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include "constants.cpp"

using namespace std;
int flag=0;
//supported raw format from user side->
//User wants to get the value corresponding to a key: GET/get "key" => key case sensitive!
//User wants to enter a new key value pair to the store : PUT/put "key" "value" => key and value both case sensitive!
//User wants to delete a key value pair : DEL/del "key" => key case sensitive!

int command_possibilities(char *t) {
    char temp[strlen(t) + 1];
    for (int i = 0; i < strlen(t); ++i) {
        temp[i] = toupper(t[i]);
    }
    temp[strlen(t)] = '\0';
    return (strcmp(temp, "GET") == 0) || (strcmp(temp, "PUT") == 0) || (strcmp(temp, "DEL") == 0);
}

void user_format() {
    if(flag == 0) {
        printf("------Correct Command format------\n");
        printf("To GET a value corresponding to key:GET/get,key\n");
        printf("To STORE a value corresponding to key:PUT/put,key,value\n");
        printf("To DELETE a value corresponding to key:DEL/del,key\n\n");
    }
}

void error() {
    printf("Error:Not a supported command\n");
    user_format();
}

void convert_xml_to_raw(char *xml) {
    char *token = strtok(xml, "\n");
    token = strtok(NULL, "\n");
    char *type = (token + 11);

    if (strcmp(type, "type=\"resp\">") == 0) {
        char *temp = strtok(NULL, "\n");
        char tempM[MAX_BUFFER_SIZE+GEN_BUFFER_SIZE];
        int i = 0;
        while (*(temp + i) != '>') {
            tempM[i] = *(temp + i);
            i++;
        }
        tempM[i] = '>';
        tempM[i + 1] = '\0';
        if (strcmp(tempM, "<Key>") == 0) {
            char key[strlen(temp)];
            temp += 5;
            i = 0;
            while (*(temp + i) != '<') {
                key[i] = *(temp + i);
                i++;
            }
            key[i] = '\0';
            temp = strtok(NULL, "\n");
            char value[strlen(temp)];
            temp += 7;
            i = 0;
            while (*(temp + i) != '<') {
                value[i] = *(temp + i);
                i++;
            }
            value[i] = '\0';
            printf("%s,%s\n", key, value);
        } else {
            temp += 9;
            i = 0;
            char message[strlen(temp)];
            while (*(temp + i) != '<') {
                message[i] = *(temp + i);
                i++;
            }
            message[i] = '\0';
            printf("%s\n", message);
        }
    } else {
        printf("XML Error:Received unparseable message");
        return;
    }
}

char *GET(char *key) {
    char p1[GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                               "<KVMessage type=\"getreq\">\n"
                               "<Key>";
    strcat(p1, key);
    char p2[] = "</Key>\n"
                "</KVMessage>";

    strcat(p1, p2);
    return strdup(p1);
}

char *PUT(char *key, char *value) {

    char p1[MAX_BUFFER_SIZE + GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                                                               "<KVMessage type=\"putreq\">\n"
                                                               "<Key>";
    strcat(p1, key);

    char p2[] = "</Key>\n"
                "<Value>";
    strcat(p1, p2);
    strcat(p1, value);
    char p3[] = "</Value>\n"
                "</KVMessage>";
    strcat(p1, p3);
    return strdup(p1);
}

char *DEL(char *key) {
    char p1[GEN_BUFFER_SIZE] = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                               "<KVMessage type=\"delreq\">\n"
                               "<Key>";
    strcat(p1, key);
    char p2[] = "</Key>\n"
                "</KVMessage>";
    strcat(p1, p2);
    return strdup(p1);
}

const char *convert_raw_to_xml(char *raw) {
    char *t = strdup(strtok(raw, ","));
    char *partitions[10];
    int count = 0;
    //partitions[count++]=t;
    try {
        while (t != NULL) {
            partitions[count++] = strdup(t);
            if (count > 3) {
                error();
                return "NULL";
            }

            t = strtok(NULL, ",");
        }
    } catch (...) {
        printf("In try catch");
        error();
        return "NULL";
    }

    if (count < 2) {
        error();
        return "NULL";
    }
    if (command_possibilities(partitions[0]) == 0) {
        error();
        return "NULL";
    }
    if (strlen(partitions[1]) > MAX_KEY_SIZE) {
        printf("Error:Oversized key\n");
        return "NULL";
    }
    if (count > 2) {
        if (strlen(partitions[2]) > MAX_VALUE_SIZE) {
            printf("Error:Oversized value\n");
            return "NULL";
        }
    }
    if ((strcmp(partitions[0], "GET") == 0) || (strcmp(partitions[0], "get") == 0)) {
        if (count > 2) {
            error();
            return "NULL";
        }
        return GET(partitions[1]);
    }
    if ((strcmp(partitions[0], "DEL") == 0) || (strcmp(partitions[0], "del") == 0)) {
        if (count > 2) {
            error();
            return "NULL";
        }

        return DEL(partitions[1]);
    }
    if ((strcmp(partitions[0], "PUT") == 0) || (strcmp(partitions[0], "put") == 0)) {
        if (count != 3) {
            error();
            return "NULL";
        }
        //printf("%ld",strlen(partitions[2]));
        //printf("%s",partitions[2]);
        return PUT(partitions[1], partitions[2]);
    }
    return "NULL";
}
//int main(){
//    char buffer[GEN_BUFFER_SIZE+MAX_VALUE_SIZE + MAX_KEY_SIZE];
//    scanf("%s",buffer);
//    convert_raw_to_xml(buffer);
//
//
//    return 0;
//}