#include "outbox.h"

void outbox_init(struct outbox *queue) {
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    pthread_mutex_init(&queue->mutex, NULL);
    pthread_cond_init(&queue->not_full, NULL);
}

void outbox_destroy(struct outbox *queue) {
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->not_full);
}

void outbox_push(struct outbox *queue, const struct outbox_item *item) {
    pthread_mutex_lock(&queue->mutex);
    while (queue->count == OUTBOX_CAPACITY) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }
    queue->items[queue->tail] = *item;
    queue->tail = (queue->tail + 1) % OUTBOX_CAPACITY;
    queue->count++;
    pthread_mutex_unlock(&queue->mutex);
}

int outbox_pop(struct outbox *queue, struct outbox_item *item) {
    int popped = 0;

    pthread_mutex_lock(&queue->mutex);
    if (queue->count > 0) {
        *item = queue->items[queue->head];
        queue->head = (queue->head + 1) % OUTBOX_CAPACITY;
        queue->count--;
        popped = 1;
        pthread_cond_signal(&queue->not_full);
    }
    pthread_mutex_unlock(&queue->mutex);
    return popped;
}
