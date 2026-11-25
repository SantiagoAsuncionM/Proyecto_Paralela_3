#pragma once

#include <pthread.h>

#define OUTBOX_CAPACITY 32
#define MPI_BODY_MAX 65536

//Message struct, MPI Cycle send them
struct outbox_item {
    int dest;
    int tag;
    int req_id;
    int len;
    char body[MPI_BODY_MAX];
};

//Messages outbox (Producer-Consumer Queue, producers = client threads, consumer = MPI Cycle)
struct outbox {
    struct outbox_item items[OUTBOX_CAPACITY];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
};

void outbox_init(struct outbox *queue);
void outbox_destroy(struct outbox *queue);

//push message copy into the queue, waits if it's full
void outbox_push(struct outbox *queue, const struct outbox_item *item);

//pop oldest message, return 1 if pop a message, 0 if queue empty
int outbox_pop(struct outbox *queue, struct outbox_item *item);
