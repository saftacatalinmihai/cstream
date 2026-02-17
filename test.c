#define CSTREAM_IMPLEMENTATION
#include "cstream.h"
#include "assert.h"

#define RED   "\x1B[31m"
#define GRN   "\x1B[32m"
#define YEL   "\x1B[33m"
#define BLU   "\x1B[34m"
#define MAG   "\x1B[35m"
#define CYN   "\x1B[36m"
#define WHT   "\x1B[37m"
#define RESET "\x1B[0m"

void test_3_components(void);

void test_source(void);

void test_sink(void);

int main(void) {
    test_3_components();
    test_source();
    test_sink();
    return 0;
}

typedef enum MsgType {
    MSG_TYPE_DATA = 0,
    MSG_TYPE_SIGNAL,
} MsgType;

typedef struct Message {
    char msgTypeName[35];
    MsgType msgType;
    void* data;
} Message;

typedef struct DomainMessage {
    i8 idx;
    i64 i1;
    i64 i2;
    b8 b1;
    char str1[32];
} DomainMessage;

void* process_control(__attribute__((unused)) Component *comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

void* process_message_c1(__attribute__((unused)) Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    dm->i2 = dm->i1 + 1;
    dm->i1 = dm->i1 + 1000;

    printf("C1 Domain Message processed: i1=%lld, i2=%lld\n", dm->i1, dm->i2);
    return msg;
}

Component* C1(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "CD1", arena,
        (void *(*)(Component *, void *))process_message_c1,
        (void *(*)(Component *, void *))process_control,
        4,
        NULL);
}

void* process_message_c2(__attribute__((unused)) Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    dm->b1 = dm->i1 % 2 == 0;

    printf("C2 Domain Message processed: i1=%lld, b1=%d\n", dm->i1, dm->b1);
    return msg;
}

Component* C2(Arena *arena) {
    Component* c = 
     COMP_FLOW(Message*, Message*, b8, "CD2", arena,
        (void *(*)(Component *, void *))process_message_c2,
        (void *(*)(Component *, void *))process_control,
        4,
        NULL);
    Component_Flow_Map(c, 0, (void *(*)(Component *, void *))process_message_c1);
    Component_Flow_Map(c, 0, (void *(*)(Component *, void *))process_message_c1);
    Component_Flow_Map(c, 0, (void *(*)(Component *, void *))process_message_c1);
    Component_Flow_Map(c, 0, (void *(*)(Component *, void *))process_message_c1);
    return c;
}

void* process_message_c3(__attribute__((unused)) Component *comp, Message** msg) {
    DomainMessage *dm =(*msg)->data;
    snprintf(dm->str1, sizeof(dm->str1), "Str Value: %lld", dm->i1);

    printf("C3 Domain Message processed: i1=%lld, str1=%s\n", dm->i1, dm->str1);
    return msg;
}

Component* C3(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "CD3", arena,
        (void *(*)(Component *, void *))process_message_c3,
        (void *(*)(Component *, void *))process_control,
        4,
        NULL);
}

void test_3_components(void) {
    printf(CYN);
    Arena *arena = Arena_create(1024 * 1024 * 1024);

    Component *c1 = C1(arena);
    Component *c2 = C2(arena);
    Component *c3 = C3(arena);

    c2->data_in[0] = c1->data_out[0];

    c3->data_in[0] = c2->data_out[0];
    Component_start(c1);
    Component_start(c2);
    Component_start(c3);

    DomainMessage dm1 = {.idx = 1, .i1 = 42};
    Message m1 = {.data = &dm1};
    void* m1_ptr = &m1;
    Port_push(c1->data_in[0], &m1_ptr, sizeof(Message*));

    DomainMessage dm2 = {.idx = 2, .i1 = 43};
    Message m2 = {.data = &dm2};
    void* m2_ptr = &m2;
    Port_push(c1->data_in[0], &m2_ptr, sizeof(Message*));

    DomainMessage dm3 = {.idx = 3, .i1 = 44};
    Message m3 = {.data = &dm3};
    void* m3_ptr = &m3;
    Port_push(c1->data_in[0], &m3_ptr, sizeof(Message*));

    int i = 0;
    while(i < 3) {
        Message* ret;
        u64 out_bytes = Port_pull(c3->data_out[0], &ret, sizeof(Message*));
        if (out_bytes > 0) {
            DomainMessage* m = (DomainMessage*)ret->data;
            printf("Final Domain Message: idx: %hhd, i1=%lld, i2=%lld, b1=%d, str1=%s\n", m->idx, m->i1, m->i2, m->b1, m->str1);

            switch (m->idx) {
                case 1:
                    assert(m->i1 == 5042);
                    assert(m->i2 == 4043);
                    assert(m->b1 == true);
                    assert(strcmp(m->str1, "Str Value: 5042") == 0);
                    break;
                case 2:
                    assert(m->i1 == 5043);
                    assert(m->i2 == 4044);
                    assert(m->b1 == false);
                    assert(strcmp(m->str1, "Str Value: 5043") == 0);
                    break;
                case 3:
                    assert(m->i1 == 5044);
                    assert(m->i2 == 4045);
                    assert(m->b1 == true);
                    assert(strcmp(m->str1, "Str Value: 5044") == 0);
                    break;
                default:
                    printf("Unknown message idx: %d\n", m->idx);
            }
            i++;
        } else {
            usleep(20000);
        }
    }
    printf(GRN "> Flow test done.\n" RESET);

    b8 stop = true;
    Component_push_control(c1, &stop, sizeof(b8));
    Component_push_control(c2, &stop, sizeof(b8));
    Component_push_control(c3, &stop, sizeof(b8));
    Component_wait_end(c1);
    Component_wait_end(c2);
    Component_wait_end(c3);

    Arena_destroy(arena);
}

void* process_tick(Component* comp, u8* tick) {
    if (DEBUG) printf("Process_tick component %s, threadID: %lu\n", comp->name, (unsigned long)pthread_self());
    return tick;
}

void test_source(void) {
    printf(CYN);
    Arena *arena = Arena_create(1024 * 1024);

    Component *comp_source = Component_Source_tick(
        "Source",
        arena,
        (void *(*)(Component *, void *))process_tick,
        sizeof(b8),
        (void *(*)(Component *, void *))process_control,
        sizeof(b8),
        10000, 
        NULL
    );

    ComponentPort *wait_port = Port_create(arena, sizeof(b8));
    comp_source->data_out[0] = wait_port;

    Component_start(comp_source);

    printf("Waiting for all data to be processed...\n");
    int i = 0;
    b8 done = false;
    while (i < 5) {
        u64 received = Port_pull(wait_port, (void*)&done, sizeof(b8));
        if (received > 0) {
            i += received / sizeof(b8);
            if (DEBUG) printf("Received done signal. Total done: %u\n", i);
        } else {
            usleep(10000); // Sleep for a short time to avoid busy waiting
        }
    }
    printf(GRN "> Source component test done.\n" RESET);

    Component_push_control(comp_source, (void*)&done, sizeof(b8));
    Component_wait_end(comp_source);

    Arena_destroy(arena);
}

typedef struct OutputData {
    u8* xs;
    u32 count;
} OutputData;

void* process_sink(Component* comp, u8* x) {
    if (*x == 10) {
        printf("Sink received 10, stopping component.\n");
        b8 stop = true;
        Component_push_control(comp, (void*)&stop, sizeof(b8));
        return NULL;
    }
    OutputData* output = (OutputData*)comp->extra_data;
    output->xs[output->count++] = *x;
    return NULL;
}

void test_sink(void) {
    printf(CYN);
    Arena *arena = Arena_create(1024 * 1024 );

    u8 input_nums[] = {1,2,3,4,5,6, 7, 8, 9, 10};
    OutputData output = {0};
    output.xs = (u8*)Arena_alloc(arena, 10 * (sizeof(u8)));

    Component *comp_source = COMP_SINK( u8, b8, "Sink", arena, process_sink, process_control, 1, &output);
    Component_start(comp_source);

    for (int i = 0; i < 10; ++i) {
        Port_push(comp_source->data_in[0], &input_nums[i], sizeof(u8));
    }

    Component_wait_end(comp_source);

    for (u32 i = 0; i < output.count; ++i) { assert(output.xs[i] == i + 1); }
    printf(GRN "> Sink component test done.\n" RESET);

    Arena_destroy(arena);
}
