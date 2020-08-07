#include "iostream"
#include <vector>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_map>
#include <bits/unordered_map.h>
#include "constants.cpp"

#define arg_access(p) ((struct arg*)p)

using namespace std;

hash<string> hasher;

struct arg {
    const char *command;
    const char *key;
    const char *value;
    const char *result_value;
    const char *response;
};

int num_of_sets;
int num_of_entries;
vector<pthread_mutex_t> cache_set_locks;
vector<pthread_cond_t> cache_set_cond;
vector<unordered_map<string, string>> cache_set;
vector<unordered_map<int, string>> cache_set_ind_to_str;//replacement
vector<unordered_map<string, int>> cache_set_str_to_ind;//replacement
vector<vector<int>> flag;//replacement
vector<int> ptr;//replacement


//store
string temp_key;
string temp_value;
const char *key_pattern = "<Key>";
const char *tab_key_pattern = "\t\t<Key>";
const char *tab_val_pattern = "\t\t<Value>";
const char *key_end_pattern = "</Key>";
const char *new_key_end_pattern = "</Key>\n";
const char *new_val_end_pattern = "</Value>\n";
const char *val_end_pattern = "</Value>";
const char *val_pattern = "<Value>";
const char *tab_kvpair = "\t<KVPair>\n";
const char *tab_ekvpair = "\t</KVPair>\n";
const char *temp_kvpair = "<KVPair>";
const char *temp_ekvpair = "</KVPair>";
const char *header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                     "<KVStore>\n";

const char *footer = "</KVStore>";
vector<FILE *> file_ptr;
vector<string> filename;

void *store_get(void *p, const char *file_name) {
    FILE *fp, *fpc;
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
                temp_value = s.substr(i + strlen(val_pattern), end - (strlen(val_pattern) + 2));
                break;
            }
            if (strcmp(s.substr(i, strlen(temp_ekvpair)).c_str(), temp_ekvpair) == 0) {
                if (strcmp(arg_access(p)->key, temp_key.c_str()) == 0) {
                    arg_access(p)->result_value = strdup(temp_value.c_str());
                    arg_access(p)->response = "Success";
                    return NULL;
                }
            }

        }
    }
    arg_access(p)->result_value = "NULL";
    arg_access(p)->response = "Does not exist";
    fclose(fp);
    return NULL;
}

void *store_put(void *p, const char *file_name) {
    //printf("++++++++++++++++%s\n",arg_access(p)->value);
    int flag = 0;
    FILE *fp, *fpc;
    char copy_file_name[strlen(file_name) + 20];
    fp = fopen(file_name, "r");
    char buffer[MAX_VALUE_SIZE + GEN_BUFFER_SIZE];

    strcpy(copy_file_name, file_name);
    strcat(copy_file_name, "_copy");
    fpc = fopen(copy_file_name, "w");
    fputs(header, fpc);
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
                if (strcmp(arg_access(p)->key, temp_key.c_str()) == 0) {
                    fputs(tab_kvpair, fpc);
                    fputs(tab_key_pattern, fpc);
                    fputs(temp_key.c_str(), fpc);
                    fputs(new_key_end_pattern, fpc);
                    fputs(tab_val_pattern, fpc);
                    fputs(arg_access(p)->value, fpc);
                    fputs(new_val_end_pattern, fpc);
                    fputs(tab_ekvpair, fpc);
                    arg_access(p)->response = "Success";
                    flag = 1;
                } else {
                    fputs(tab_kvpair, fpc);
                    fputs(tab_key_pattern, fpc);
                    fputs(temp_key.c_str(), fpc);
                    fputs(new_key_end_pattern, fpc);
                    fputs(tab_val_pattern, fpc);
                    fputs(temp_value.c_str(), fpc);
                    fputs(new_val_end_pattern, fpc);
                    fputs(tab_ekvpair, fpc);

                }
            }
        }
    }
    if (flag == 0) {
        fputs(tab_kvpair, fpc);
        fputs(tab_key_pattern, fpc);
        fputs(arg_access(p)->key, fpc);
        fputs(new_key_end_pattern, fpc);
        fputs(tab_val_pattern, fpc);
        fputs(arg_access(p)->value, fpc);
        fputs(new_val_end_pattern, fpc);
        fputs(tab_ekvpair, fpc);
        arg_access(p)->response = "Success";
    }
    fputs(footer, fpc);
    fclose(fp);
    remove(file_name);
    
    rename(copy_file_name, file_name);
    fclose(fpc);
        //delete(&temp_value);
    return NULL;
}

void *store_del(void *p, const char *file_name) {

    int flag = 0;
    FILE *fp, *fpc;
    fp = fopen(file_name, "r");
    char copy_file_name[strlen(file_name) + 20];
    strcpy(copy_file_name, file_name);
    strcat(copy_file_name, "_copy");
    fpc = fopen(copy_file_name, "w");
    char buffer[MAX_VALUE_SIZE + GEN_BUFFER_SIZE];
    fputs(header, fpc);
    while (fgets(buffer, MAX_VALUE_SIZE + GEN_BUFFER_SIZE, fp) != NULL) {
        string s = buffer;
        for (int i = 0; i < strlen(buffer); i++) {
            if (strcmp(s.substr(i, strlen(key_pattern)).c_str(), key_pattern) == 0) {
                int end = s.find(key_end_pattern);
                temp_key = s.substr(i + strlen(key_pattern), end - (strlen(key_pattern) + 2));
                break;
            }
            if (strcmp(s.substr(i, strlen(val_pattern)).c_str(), val_pattern) == 0) {
                int end = s.find(val_end_pattern);
                temp_value = s.substr(i + strlen(val_pattern), end - (strlen(val_pattern) + 2));
                break;
            }
            if (strcmp(s.substr(i, strlen(temp_ekvpair)).c_str(), temp_ekvpair) == 0) {
                if (strcmp(arg_access(p)->key, temp_key.c_str()) != 0) {
                    fputs(tab_kvpair, fpc);
                    fputs(tab_key_pattern, fpc);
                    fputs(temp_key.c_str(), fpc);
                    fputs(new_key_end_pattern, fpc);
                    fputs(tab_val_pattern, fpc);
                    fputs(temp_value.c_str(), fpc);
                    fputs(new_val_end_pattern, fpc);
                    fputs(tab_ekvpair, fpc);
                }
                if (strcmp(arg_access(p)->key, temp_key.c_str()) == 0) {
                    flag = 1;//successfull delete as key,value pair is not written in the final file
                }
            }

        }
        if (flag == 1) {
            arg_access(p)->response = "Success";
        } else {
            arg_access(p)->response = "Does not exist";
        }

    }
    fputs(footer, fpc);
    fclose(fp);
    remove(file_name);
    rename(copy_file_name, file_name);
    fclose(fpc);
    cout << "in store_del!" << endl;
    return NULL;
}

void print_cache_sets(int y) {
    cout << "Contents of set " << y << " are:" << endl;
    for (auto x:cache_set[y]) {
        cout << x.first << " : " << cache_set[y][x.first] << " flag:" << flag[y][cache_set_str_to_ind[y][x.first]]
             << endl;
    }
    cout << "Contents of set " << y << " done!" << endl;

}

void initialize_cache(int noe, int nos) {
    num_of_sets = nos;
    num_of_entries = noe;
    cache_set.resize(num_of_sets);
    cache_set_str_to_ind.resize(num_of_sets);
    cache_set_ind_to_str.resize(num_of_sets);
    cache_set_cond.resize(num_of_sets);
    cache_set_locks.resize(num_of_sets);
    flag.resize(num_of_sets);
    ptr.resize(num_of_sets);
    for (int i = 0; i < num_of_sets; i++) {
        flag[i].resize(num_of_entries);
    }
    printf("Cache initialisation complete!\n");
}



int get_set_number(const char *str) {
    size_t hash = hasher(str);
    return hash % num_of_sets;
}
void initial_copy(){
    string temp_key,temp_value;
    char buffer[MAX_VALUE_SIZE + GEN_BUFFER_SIZE];

    FILE *fp;
    fp = fopen("Total", "r");
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
                struct arg* temp = (struct arg *)malloc(sizeof(struct arg));
                temp->key=temp_key.c_str();
                temp->value=temp_value.c_str();
                store_put((void *)temp,filename[get_set_number(temp->key)].c_str());
                free(temp);
            }
        }
    }
    fclose(fp);
}
void initiliase_store() {
    for (int i = 0; i < num_of_sets; i++) {
        char file[10] = "file";
        strcat(file, to_string(i).c_str());
        filename.push_back(file);
        FILE *temp = fopen(file, "w");
        if (temp != NULL) {
            //file_ptr.push_back(temp);
            fputs(header, temp);
            fputs(footer, temp);
            fclose(temp);
            printf("File %s created for store\n", file);
        }
    }
    if( access( "Total", F_OK ) != -1 ) {
        // file exists
        initial_copy();
    } else {
        // file doesn't exist
        FILE *temp = fopen("Total", "w");
        fputs(header, temp);
        fputs(footer, temp);
        fclose(temp);
    }
    printf("Store initialisation complete!\n");
}

void *PUT(void *p) {
    int cache_set_number = get_set_number(((struct arg *) p)->key);
    pthread_mutex_lock(&(cache_set_locks[cache_set_number]));
    //printf("in storeput+++++++++++++++++++++++++++++++++++++++++\n");
    if (!(cache_set[cache_set_number].find(arg_access(p)->key) == cache_set[cache_set_number].end())) {
        cout << "Key Value pair already present in cache!" << endl;
        cout << "Updating Value for given key!" << endl;
        cache_set[cache_set_number][arg_access(p)->key] = arg_access(p)->value;
        pthread_mutex_unlock(&(cache_set_locks[cache_set_number]));
    }
    struct arg* temp_p = (struct arg *)malloc(sizeof(struct arg));
    temp_p->key=arg_access(p)->key;
    temp_p->value=arg_access(p)->value;
    store_put(p, filename[cache_set_number].c_str());
    store_put((void *)temp_p,"Total");
    free(temp_p);
    pthread_mutex_unlock(&(cache_set_locks[cache_set_number]));
    //printf("end of store\n");
    return NULL;
}

void *DEL(void *p) {
    int cache_set_number = get_set_number(((struct arg *) p)->key);
    struct arg* temp_p = (struct arg *)malloc(sizeof(struct arg));
    temp_p->key=arg_access(p)->key;
    pthread_mutex_lock(&(cache_set_locks[cache_set_number]));
    if (!(cache_set[cache_set_number].find(arg_access(p)->key) == cache_set[cache_set_number].end())) {
        cout << "Key Value pair already present in cache!" << endl;
        cout << "Deleting Value for given key!" << endl;

        cache_set[cache_set_number].erase(arg_access(p)->key);
        store_del(p, filename[cache_set_number].c_str());
        //store_del((void *)temp_p,"Total");
        cout << "End of DEL!" << endl;
        arg_access(p)->response = "Success";
    } else {
        cout << "Key not found in cache set!";
        store_del(p, filename[cache_set_number].c_str());
        //store_del((void *)temp_p,"Total");
        //arg_access(p)->response = "Does not exist";
    }
    store_del((void *)temp_p,"Total");
    free(temp_p);

    pthread_mutex_unlock(&(cache_set_locks[cache_set_number]));

    return NULL;

}

void *GET(void *p) {
    int cache_set_number = get_set_number(arg_access(p)->key);
    pthread_mutex_lock(&(cache_set_locks[cache_set_number]));

    if (cache_set[cache_set_number].find(arg_access(p)->key) == cache_set[cache_set_number].end()) {
        cout << "Cache miss" << endl;
        cout << "Miss:" << arg_access(p)->key << endl;
        store_get((void *) p, filename[cache_set_number].c_str());
        if (strcmp(arg_access(p)->response, "Success") == 0) {

            if (cache_set[cache_set_number].size() < num_of_entries) {
                cache_set_str_to_ind[cache_set_number][arg_access(p)->key] = cache_set[cache_set_number].size();
                cache_set_ind_to_str[cache_set_number][cache_set[cache_set_number].size()] = arg_access(p)->key;
                cache_set[cache_set_number][arg_access(p)->key] = arg_access(p)->result_value;
            } else {
                //replacement to be done here
                while (flag[cache_set_number][ptr[cache_set_number]] != 0) {
                    flag[cache_set_number][ptr[cache_set_number]] = 0;
                    ptr[cache_set_number] = (ptr[cache_set_number] + 1) % num_of_entries;

                }
                cache_set[cache_set_number].erase(cache_set_ind_to_str[cache_set_number][ptr[cache_set_number]]);
                cache_set_str_to_ind[cache_set_number][arg_access(p)->key] = ptr[cache_set_number];
                cache_set_ind_to_str[cache_set_number][ptr[cache_set_number]] = arg_access(p)->key;
                cache_set[cache_set_number][arg_access(p)->key] = arg_access(p)->result_value;
                ptr[cache_set_number] = (ptr[cache_set_number] + 1) % num_of_entries;
            }
        }
    } else {
        flag[cache_set_number][cache_set_str_to_ind[cache_set_number][arg_access(p)->key]] = 1;
        arg_access(p)->result_value = cache_set[cache_set_number][arg_access(p)->key].c_str();
        arg_access(p)->response = "Success";
        cout << "Cache hit" << endl;
    }
    pthread_mutex_unlock(&(cache_set_locks[cache_set_number]));

    return NULL;
}

void *process_request(void *p) {
    //printf("Here");
    //puts(p);
    //printf("In kv cache process request\n");
    if (strcmp(arg_access(p)->command, "get") == 0) {
        //printf("In kv cache get\n");
        return GET(p);
    } else if (strcmp(arg_access(p)->command, "put") == 0) {
        //printf("in KV cassdadd\n");
        return PUT(p);
    } else if (strcmp(arg_access(p)->command, "del") == 0) {
        return DEL(p);
    }
    return NULL;
}

int mains() {
    initialize_cache(2,4);
    initiliase_store();
    /*printf("%d",get_set_number("hello4"));
    struct arg* a=(struct arg * )malloc(sizeof(struct arg));
    a->key="hello";
    a->value="world";
    PUT((void *)a);
    a=(struct arg * )malloc(sizeof(struct arg));
    a->key="helloshit";
    a->value="world3";
    PUT((void *)a);*/
}