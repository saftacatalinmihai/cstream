#define CSTREAM_IMPLEMENTATION
#include "cstream.h"
#include "assert.h"

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

void* process_control(Component *comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

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
            printf("Done\n");
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
    printf("Domain message processing test done.\n");

}
