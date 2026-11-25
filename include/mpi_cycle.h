#pragma once

#define MPI_REQUEST_TIMEOUT_SEC 5

//Message types between brokers
enum mpi_msg_type {
    MSG_REPLY = 1,
    MSG_PING  = 2
    //TODO: other msg cases
};

//Prepares outbox queue and pending table
void mpi_cycle_init(void);

//Main thread loop, only one calling MPI
//Sends messages in outbox, receives and handles messages from other brokers
void mpi_cycle_run(void);

//Cycle stop request
void mpi_cycle_stop(void);

//Sends a message to another broker, waits for the reply
//Returns reply length, -1 on error or timeout
int mpi_request(int dest, int type, const void *body, int len,
                void *reply, int reply_max);

//Sends a message to another broker, don't wait for reply
//Returns 0 if queued, -1 if body too long
int mpi_send_async(int dest, int type, const void *body, int len);