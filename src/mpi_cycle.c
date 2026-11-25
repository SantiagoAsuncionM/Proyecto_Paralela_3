#include "mpi_cycle.h"
#include "outbox.h"

#include <errno.h>
#include <mpi.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_PENDING 64
#define MAX_SENDING 256

//MPI message header
struct mpi_header {
    int req_id;
    int len;
};

//Client thread waiting for reply
struct pending {
    int used;
    int req_id;
    int done;
    char *reply;
    int reply_max;
    int reply_len;
    pthread_cond_t cond;
};

//Message with send in process
struct sending {
    char *buffer;
    MPI_Request request;
};

static struct outbox g_outbox;
static struct outbox_item g_item;
static char g_recv_buffer[sizeof(struct mpi_header) + MPI_BODY_MAX];

static struct pending g_pending[MAX_PENDING];
static pthread_mutex_t g_pending_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_next_req_id = 1;

static struct sending g_sending[MAX_SENDING];
static int g_sending_count = 0;

static int g_stop = 0;
static pthread_mutex_t g_stop_mutex = PTHREAD_MUTEX_INITIALIZER;

void mpi_cycle_init(void) {
    outbox_init(&g_outbox);
    for (int i = 0; i < MAX_PENDING; i++) {
        g_pending[i].used = 0;
        pthread_cond_init(&g_pending[i].cond, NULL);
    }
}

void mpi_cycle_stop(void) {
    pthread_mutex_lock(&g_stop_mutex);
    g_stop = 1;
    pthread_mutex_unlock(&g_stop_mutex);
}

static int stop_requested(void) {
    pthread_mutex_lock(&g_stop_mutex);
    int stop = g_stop;
    pthread_mutex_unlock(&g_stop_mutex);
    return stop;
}

int mpi_send_async(int dest, int type, const void *body, int len) {
    if (len < 0 || len > MPI_BODY_MAX) {
        return -1;
    }

    struct outbox_item *item = malloc(sizeof(struct outbox_item));
    if (item == NULL) {
        return -1;
    }
    item->dest = dest;
    item->tag = type;
    item->req_id = 0;
    item->len = len;
    if (len > 0) {
        memcpy(item->body, body, len);
    }
    outbox_push(&g_outbox, item);
    free(item);
    return 0;
}

int mpi_request(int dest, int type, const void *body, int len,
                void *reply, int reply_max) {
    if (len < 0 || len > MPI_BODY_MAX) {
        return -1;
    }

    pthread_mutex_lock(&g_pending_mutex);
    int slot = -1;
    for (int i = 0; i < MAX_PENDING && slot < 0; i++) {
        if (!g_pending[i].used) {
            slot = i;
        }
    }
    if (slot < 0) {
        pthread_mutex_unlock(&g_pending_mutex);
        return -1;
    }
    struct pending *entry = &g_pending[slot];
    entry->used = 1;
    entry->req_id = g_next_req_id++;
    entry->done = 0;
    entry->reply = reply;
    entry->reply_max = reply_max;
    entry->reply_len = 0;
    int req_id = entry->req_id;
    pthread_mutex_unlock(&g_pending_mutex);

    struct outbox_item *item = malloc(sizeof(struct outbox_item));
    if (item == NULL) {
        pthread_mutex_lock(&g_pending_mutex);
        entry->used = 0;
        pthread_mutex_unlock(&g_pending_mutex);
        return -1;
    }
    item->dest = dest;
    item->tag = type;
    item->req_id = req_id;
    item->len = len;
    if (len > 0) {
        memcpy(item->body, body, len);
    }
    outbox_push(&g_outbox, item);
    free(item);

    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += MPI_REQUEST_TIMEOUT_SEC;

    pthread_mutex_lock(&g_pending_mutex);
    int rc = 0;
    while (!entry->done && rc != ETIMEDOUT) {
        rc = pthread_cond_timedwait(&entry->cond, &g_pending_mutex, &deadline);
    }
    int result = entry->done ? entry->reply_len : -1;
    entry->used = 0;
    pthread_mutex_unlock(&g_pending_mutex);
    return result;
}

static void complete_pending(int req_id, const char *body, int len) {
    pthread_mutex_lock(&g_pending_mutex);
    for (int i = 0; i < MAX_PENDING; i++) {
        struct pending *entry = &g_pending[i];
        if (entry->used && entry->req_id == req_id && !entry->done) {
            int copy = len < entry->reply_max ? len : entry->reply_max;
            memcpy(entry->reply, body, copy);
            entry->reply_len = copy;
            entry->done = 1;
            pthread_cond_signal(&entry->cond);
            break;
        }
    }
    pthread_mutex_unlock(&g_pending_mutex);
}

static int send_now(int dest, int tag, int req_id, const void *body, int len) {
    if (g_sending_count == MAX_SENDING) {
        return -1;
    }

    int size = (int) sizeof(struct mpi_header) + len;
    char *buffer = malloc(size);
    if (buffer == NULL) {
        return -1;
    }
    struct mpi_header header = { req_id, len };
    memcpy(buffer, &header, sizeof(header));
    if (len > 0) {
        memcpy(buffer + sizeof(header), body, len);
    }

    struct sending *send = &g_sending[g_sending_count];
    send->buffer = buffer;
    MPI_Isend(buffer, size, MPI_BYTE, dest, tag, MPI_COMM_WORLD, &send->request);
    g_sending_count++;
    return 0;
}

static void check_sending(void) {
    int i = 0;
    while (i < g_sending_count) {
        int finished = 0;
        MPI_Test(&g_sending[i].request, &finished, MPI_STATUS_IGNORE);
        if (finished) {
            free(g_sending[i].buffer);
            g_sending[i] = g_sending[g_sending_count - 1];
            g_sending_count--;
        } else {
            i++;
        }
    }
}

static void receive_and_handle(MPI_Status *status) {
    int bytes = 0;
    MPI_Get_count(status, MPI_BYTE, &bytes);
    MPI_Recv(g_recv_buffer, bytes, MPI_BYTE, status->MPI_SOURCE, status->MPI_TAG,
             MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (bytes < (int) sizeof(struct mpi_header)) {
        return;
    }
    struct mpi_header header;
    memcpy(&header, g_recv_buffer, sizeof(header));
    if (header.len < 0 || header.len > bytes - (int) sizeof(header)) {
        return;
    }
    char *body = g_recv_buffer + sizeof(header);

    switch (status->MPI_TAG) {
    case MSG_REPLY:
        complete_pending(header.req_id, body, header.len);
        break;
    case MSG_PING:
        send_now(status->MPI_SOURCE, MSG_REPLY, header.req_id, "PONG", 4);
        break;
    default:
        break;
    }
}

void mpi_cycle_run(void) {
    struct timespec pause = { 0, 100000 };

    while (1) {
        int worked = 0;

        while (g_sending_count < MAX_SENDING && outbox_pop(&g_outbox, &g_item)) {
            send_now(g_item.dest, g_item.tag, g_item.req_id, g_item.body, g_item.len);
            worked = 1;
        }

        int arrived = 0;
        MPI_Status status;
        MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &arrived, &status);
        if (arrived) {
            receive_and_handle(&status);
            worked = 1;
        }

        check_sending();

        if (!worked && g_sending_count == 0 && stop_requested()) {
            break;
        }
        if (!worked) {
            nanosleep(&pause, NULL);
        }
    }
}