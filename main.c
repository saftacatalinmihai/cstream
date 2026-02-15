#include "cstream.h"

#define VER 1

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

void* process_control(Component *comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

#define NUM_COMPS 8
#define NUM_DATA 1000042

typedef struct DomainMessage {
    i64 i1;
    i64 i2;
    b8 b1;
    char str1[32];
} DomainMessage;

void* process_message_c1(Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    dm->i2 = dm->i1 + 1;
    dm->i1 = dm->i1 + 1000;

    printf("C1 Domain Message processed: i1=%lld, i2=%lld\n", dm->i1, dm->i2);
    return msg;
}

Component* C1(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "CD1", arena,
        (void *)process_message_c1,
        (void *)process_control,
        4,
        NULL);
}

void* process_message_c2(Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    dm->b1 = dm->i1 % 2 == 0;

    printf("C2 Domain Message processed: i1=%lld, b1=%d\n", dm->i1, dm->b1);
    return msg;
}

Component* C2(Arena *arena) {
    Component* c = 
     COMP_FLOW(Message*, Message*, b8, "CD2", arena,
        (void *)process_message_c2,
        (void *)process_control,
        4,
        NULL);
    Component_Flow_Map(c, 0, (void*)process_message_c1);
    Component_Flow_Map(c, 0, (void*)process_message_c1);
    Component_Flow_Map(c, 0, (void*)process_message_c1);
    Component_Flow_Map(c, 0, (void*)process_message_c1);
    return c;
}

void* process_message_c3(Component *comp, Message** msg) {
    DomainMessage *dm =(*msg)->data;
    snprintf(dm->str1, sizeof(dm->str1), "Str Value: %lld", dm->i1);

    printf("C3 Domain Message processed: i1=%lld, str1=%s\n", dm->i1, dm->str1);
    return msg;
}

Component* C3(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "CD3", arena,
        (void *)process_message_c3,
        (void *)process_control,
        4,
        NULL);
}

int main() {
    Arena *arena = Arena_create(1024 * 1024 * 1024);

    Component *c1 = C1(arena);
    Component *c2 = C2(arena);
    Component *c3 = C3(arena);
    Component_start(c1);
    Component_start(c2);
    Component_start(c3);

    c2->data_in[0] = c1->data_out[0];
    c3->data_in[0] = c2->data_out[0];

    DomainMessage dm1 = {.i1 = 42};
    Message m1 = {.data = &dm1};
    void* m1_ptr = &m1;
    Port_push(c1->data_in[0], &m1_ptr, sizeof(Message*));

    DomainMessage dm2 = {.i1 = 43};
    Message m2 = {.data = &dm2};
    void* m2_ptr = &m2;
    Port_push(c1->data_in[0], &m2_ptr, sizeof(Message*));

    DomainMessage dm3 = {.i1 = 44};
    Message m3 = {.data = &dm3};
    void* m3_ptr = &m3;
    Port_push(c1->data_in[0], &m3_ptr, sizeof(Message*));

    int i = 0;
    while(i < 3) {
        Message* ret;
        u64 out_bytes = Port_pull(c3->data_out[0], &ret, sizeof(Message*));
        if (out_bytes > 0) {
            printf("Done\n");
            printf("Final Domain Message: i1=%lld, i2=%lld, b1=%d, str1=%s\n", ((DomainMessage*)ret->data)->i1, ((DomainMessage*)ret->data)->i2, ((DomainMessage*)ret->data)->b1, ((DomainMessage*)ret->data)->str1);
            i++;
        } else {
            if (DEBUG) printf("No output yet...\n");
            usleep(20000);
        }
    }
    printf("Domain message processing test done.\n");
    getchar();

    // Prev
    struct timespec start, t1, t2, t3, end;

    clock_gettime(CLOCK_MONOTONIC, &start);

    Component *comps[NUM_COMPS] = {0};
    char names[NUM_COMPS][5] = {0};
    for (int i = 0; i < NUM_COMPS; ++i) {
        sprintf(names[i], "C%d", i + 1);
        Component *comp = COMP_FLOW(
            Message*,
            Message*,
            b8,
            names[i], arena,
            (void *)process_i64,
            (void *)process_control,
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
        (void*)consume_i64,
        (void*)process_control,
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
        if (DEBUG) printf("Wait received %llu bytes\n", received);
        if (received > 0) {
            done_received += received / sizeof(b8);
            if (DEBUG) printf("Received done signal. Total done: %u/%u\n", done_received, NUM_DATA);
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

