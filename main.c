/* #define DEBUG true */
#define DEBUG false

#define CSTREAM_IMPLEMENTATION
#include "cstream.h"


typedef enum MsgType {
    MSG_TYPE_DATA = 0,
    MSG_TYPE_SIGNAL,
} MsgType;

typedef enum SignalType {
    SIGNAL_TYPE_STOP,
} SignalType;

typedef struct SignalMessage1 {
    SignalType signalType;
} SignalMessage;

typedef struct Message {
    char msgTypeName[35];
    MsgType msgType;
    void* data;
} Message;

void* process_i64(Component *comp, Message** message) {
    Message *msg = *message;

    if ((i64)msg->data == -1) {
        printf("-1\n");
    }
    usleep(100);
     if (DEBUG) printf("[Component: %s, threadID: %lu] Got Value: %lld\n", comp->name, (unsigned long)pthread_self(), *(i64*)(msg->data));

    i64 new_value = *(i64*)(msg->data) + 1000;
    *(i64*)(msg->data) = new_value;

    /* Port_push(comp->data_out[0], (char*)msg, sizeof(Message)); */
    Port_data_out_push(comp, 0, &msg, sizeof(Message*));
    if (DEBUG)  printf("[Component: %s, threadID: %lu] Produced Value: %lld\n", comp->name, (unsigned long)pthread_self(), new_value);
    return NULL;
}

void* consume_i64(Component *comp, Message** message) {
    Message *msg = *message;

    ComponentPort *wait_port = comp->extra_data;
    if (DEBUG)  printf("[Component: %s, threadID: %lu] Consumed Value: %lld\n", comp->name, (unsigned long)pthread_self(), *(i64*)(msg->data));
    b8 done = true;
    Port_push(wait_port, &done, sizeof(b8));
    return NULL;
}

void* process_control(__attribute__((unused)) Component *comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

#define NUM_COMPS 8
#define NUM_DATA 10042
/* #define NUM_DATA 0 */
int main(void) {
    Arena *arena = Arena_create(1024 * 1024 * 1024);

    struct timespec start, t1, t2, t3, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    Component *comps[NUM_COMPS] = {0};
    char names[NUM_COMPS][5] = {0};
    for (int i = 0; i < NUM_COMPS; ++i) {
        snprintf(names[i], 5, "C%d", i + 1);
        Component *comp = COMP_FLOW(
            Message*,
            Message*,
            b8,
            names[i], arena,
            (void *(*)(Component *, void *))process_i64,
            (void *(*)(Component *, void *))process_control,
            4, 
            NULL);
        comps[i] = comp;
        if (i > 0) {
            comps[i]->data_in[0] = comps[i - 1]->data_out[0];
        }
    }

    ComponentPort *wait_port = Port_create(arena, sizeof(b8));
    Component *comp_sink = COMP_SINK(
        Message*, b8,
        "CS",
        arena,
        (void *(*)(Component *, void *))consume_i64,
        (void *(*)(Component *, void *))process_control,
        4,
        (void*)wait_port
    );

    comp_sink->data_in[0] = comps[NUM_COMPS-1]->data_out[0];

    for (int i = 0; i < NUM_COMPS; ++i) { Component_start(comps[i]); }
    Component_start(comp_sink);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    Message *msg = calloc(NUM_DATA, sizeof(Message));
    i64 *data = calloc(NUM_DATA, sizeof(i64));

    printf("Pushing data: Bytes: %lu\n", NUM_DATA * sizeof(Message));
    u32 done_received = 0;
    b8 done = false;

    for (i64 i = 0; i < NUM_DATA; ++i) {
        data[i] = i + 1;
        strcpy(msg[i].msgTypeName, "i64");
        msg[i].msgType = MSG_TYPE_DATA;
        msg[i].data = data + i;
        void* ptr = msg + i;
        Port_push(comps[0]->data_in[0], &ptr, sizeof(Message*));
        u64 received = Port_pull(wait_port, &done, sizeof(b8));
        if (DEBUG) printf("Wait received %llu bytes\n", received);
        if (received > 0) {
            done_received += received / sizeof(b8);
            if (DEBUG) printf("Received done signal. Total done: %u/%u\n", done_received, NUM_DATA);
        }

        /* Port_data_out_push(comps[0], 0, (void*)&msgCtx[i], sizeof(Message)); */
        /* if (debug) printf("Pushed %lu size\n", sizeof(msgCtx[i])); */
        if (DEBUG) printf("Pushed %lld, %lu size\n", *(i64*)((Message*)(msg[i]).data), sizeof(Message*));
    }

    printf("Waiting for all data to be processed...\n");
    while (done_received < NUM_DATA) {
        u64 received = Port_pull(wait_port, (void*)&done, sizeof(b8));
        /* if (DEBUG) printf("Wait received %llu bytes\n", received); */
        if (received > 0) {
            done_received += received / sizeof(b8);
            if (DEBUG) printf("Received done signal. Total done: %u/%u\n", done_received, NUM_DATA);
        } else {
            usleep(1000); // Sleep for a short time to avoid busy waiting
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);
    printf("All data processed.\n");

    /* return 0; */
    
    /* sleep(1); */
    printf("Sending stop control ...\n");

    b8 stop = true;

    for (int i=0; i < NUM_COMPS; i++) { Component_push_control(comps[i], &stop, sizeof(b8)); }
    Component_push_control(comp_sink, &stop, sizeof(b8));

    printf("Stop control done.\n");

    clock_gettime(CLOCK_MONOTONIC, &t3);

    for (int i=0; i<NUM_COMPS; i++) { Component_wait_end(comps[i]); }
    Component_wait_end(comp_sink);

    printf("Components shut down.\n");

    clock_gettime(CLOCK_MONOTONIC, &end);

    printf("Duration:\n");
    printf("  Component creation: %lf seconds\n", (t1.tv_sec - start.tv_sec) + (t1.tv_nsec - start.tv_nsec) / 1e9);
    printf("  Data pushing: %lf seconds\n", (t2.tv_sec - t1.tv_sec) + (t2.tv_nsec - t1.tv_nsec) / 1e9);
    printf("  Sending stop signals: %lf seconds\n", (t3.tv_sec - t2.tv_sec) + (t3.tv_nsec - t2.tv_nsec) / 1e9);
    printf("  Component shutdown: %lf seconds\n", (end.tv_sec - t3.tv_sec) + (end.tv_nsec - t3.tv_nsec) / 1e9);
    printf("Total duration: %lf seconds\n", (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9);

    clock_gettime(CLOCK_MONOTONIC, &start);
    i64 *data2 = calloc(NUM_DATA, sizeof(i64));
    for (i64 i = 0; i < NUM_DATA; ++i) {
        data2[i] = i;
        if (data2[2] == -1) {
            printf("-1\n");
        }
        usleep(100);
    }
    clock_gettime(CLOCK_MONOTONIC, &end);

    printf("Direct data generation duration: %lf seconds\n", 
           (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9);

    free(msg);
    free(data);
    free(data2);

    return 0;
}

