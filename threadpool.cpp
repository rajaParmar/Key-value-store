#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <queue>
#include "KV_cache.cpp"
#include "server_xml.cpp"

using namespace std;

#define THREADS max_threads
int max_threads = 20;

struct pool_queue {
    struct args *req;
    int sd;
    struct pool_queue *next;
};

struct pool {
    int waiting;
    unsigned int remaining;
    unsigned int nthreads;
    struct pool_queue *q;
    struct pool_queue *end;
    pthread_mutex_t q_mutex;
    pthread_cond_t q_cond;
    pthread_t *threads;
};

static void *thread(void *p);

struct pool *create_pool() {
    struct pool *p = (struct pool *) malloc(sizeof(struct pool));// + THREADS * sizeof(pthread_t));
    pthread_mutex_init(&p->q_mutex, NULL);
    pthread_cond_init(&p->q_cond, NULL);
    p->threads = (pthread_t*)malloc(max_threads * sizeof(pthread_t));
    p->nthreads = THREADS;
    p->waiting = 0;
    p->remaining = 0;
    p->end = NULL;
    p->q = NULL;

    for (int i = 0; i < THREADS; i++) {
        pthread_create(&p->threads[i], NULL, &thread, p);
    }
    return p;
}

struct pool *create_pool(int threads) {
    max_threads = threads;
    return create_pool();
}

void place_request(struct pool *p, char *Buffer, int sd) {
    struct args *reqArgs = (struct args *) malloc(sizeof(struct args));
    reqArgs = (struct args *) convert_xml_to_raw(Buffer, reqArgs);
    //("cmd=%s key=%s  VAl=%s",reqArgs->command,reqArgs->key, reqArgs->value);
    struct pool_queue *poolQueue = (struct pool_queue *) malloc(sizeof(struct pool_queue));
    poolQueue->req = reqArgs;
    poolQueue->next = NULL;
    poolQueue->sd = sd;

    pthread_mutex_lock(&p->q_mutex);

    if (p->q == NULL) {
        p->q = p->end = poolQueue;
    } else {
        p->end->next = poolQueue;
        p->end = poolQueue;
    }
    p->remaining++;
    pthread_cond_signal(&p->q_cond);

    pthread_mutex_unlock(&p->q_mutex);
}

void place_request(struct pool *p, struct args *req) {
    struct pool_queue *poolQueue = (struct pool_queue *) malloc(sizeof(struct pool_queue));
    poolQueue->req = req;
    poolQueue->next = NULL;

    pthread_mutex_lock(&p->q_mutex);

    if (p->q == NULL) {
        p->q = p->end = poolQueue;
    } else {
        p->end->next = poolQueue;
        p->end = poolQueue;
    }
    p->remaining++;
    pthread_cond_signal(&p->q_cond);

    pthread_mutex_unlock(&p->q_mutex);
}

int send_all(int socket, void *buffer, size_t length)
{
    char *ptr = (char*) buffer;
    while (length > 0)
    {
        int i = send(socket, ptr, length,0);
        if (i < 1) return -1;
        ptr += i;
        length -= i;
    }
    return 0;
}

void process_req(struct pool *p) {
    char *buffer;
    //printf("1\n");
    struct pool_queue *poolQueue = p->q;
    if (p->q == p->end) {
        p->end = NULL;
    }
    p->q = p->q->next;
    //printf("2\n");
    process_request((void *) (poolQueue->req));
    //printf("After process_Request\n");

    if (strcmp(poolQueue->req->response, "Success") == 0) {
        if (strcmp(poolQueue->req->command, "get") == 0) {
            buffer = GET_RES(poolQueue->req->key, poolQueue->req->result_value);
        } else if (strcmp(poolQueue->req->command, "put") == 0) {
            buffer = PUT_RES();
        } else if (strcmp(poolQueue->req->command, "del") == 0) {
            buffer = DEL_RES();
        }
    } else {
        buffer = ERROR_RES(poolQueue->req->response);
    }
    puts(buffer);
    //printf("sendind==%d \n %s",(int)strlen(buffer),buffer);
    send(poolQueue->sd, buffer, strlen(buffer),0);
//    int res = send_all(poolQueue->sd, buffer, strlen(buffer));
//    if (res < 0) {
//        perror("Network Error:Could not send data");
//    }
}

static void *thread(void *args) {
    struct pool *p = (struct pool *) args;

    while (true) {
        pthread_mutex_lock(&p->q_mutex);

        pthread_cond_wait(&p->q_cond, &p->q_mutex);
        //printf("Making call to process req\n");
        process_req(p);

        pthread_mutex_unlock(&p->q_mutex);
    }
}
