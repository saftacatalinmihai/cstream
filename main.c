#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>
#include <time.h>
#include <string.h>

typedef int8_t  i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef u8 b8;

b8 debug = false;
/* b8 debug = true; */

typedef struct Arena {
    char* memory;
    u64 size;
    u64 offset;
} Arena;

void* Arena_alloc_with_type(Arena* arena, u64 size, char* custom_name);

#define ARENA_ALLOC(arena, T) Arena_alloc_with_type(arena, sizeof(T), TYPE_##T, #T)
#define ARENA_ALLOC_NAMED(arena, T, name) Arena_alloc_with_type(arena, sizeof(T), TYPE_##T, name)

Arena* Arena_create(u64 size) {
    Arena* arena = malloc(sizeof(Arena));
    arena->memory = malloc(size);
    arena->size = size;
    arena->offset = 0;
    return arena;
}

void* Arena_alloc(Arena* arena, u64 size) {
    return Arena_alloc_with_type(arena, size, NULL);
}

void* Arena_alloc_with_type(Arena* arena, u64 size, char* custom_name) {
    if (arena->offset + size > arena->size) {
        return NULL;
    }

    void* ptr = arena->memory + arena->offset;
    arena->offset += size;
    return ptr;
}

void Arena_reset(Arena* arena) {
    arena->offset = 0;
}

void Arena_destroy(Arena* arena) {
    free(arena->memory);
    free(arena);
}

void Arena_print_memory(Arena* arena) {
    for (u64 i = 0; i < arena->offset; i++) {
        printf("%02x ", (unsigned char)arena->memory[i]);
    }
    printf("\n");
}

typedef struct ComponentPort { // A ring buffer for data
    char* buffer;
    u64 head;
    u64 tail;
    u64 max; //of the buffer
    b8 full;
    pthread_mutex_t *mutex;
    pthread_cond_t *cond_not_full;
    pthread_cond_t *cond_not_empty;
} ComponentPort;

ComponentPort * Port_create(Arena *arena, u16 data_size) {
    ComponentPort *port = Arena_alloc(arena, sizeof(ComponentPort));
    port->max = data_size * 16;
    port->buffer = Arena_alloc(arena, data_size * port->max);
    port->mutex = (pthread_mutex_t*) Arena_alloc(arena,sizeof(pthread_mutex_t));
    pthread_mutex_init(port->mutex, NULL);
    port->cond_not_full =  (pthread_cond_t*)Arena_alloc(arena, sizeof(pthread_cond_t));
    pthread_cond_init(port->cond_not_full, NULL);
    port->cond_not_empty =  (pthread_cond_t*)Arena_alloc(arena, sizeof(pthread_cond_t));
    pthread_cond_init(port->cond_not_empty, NULL);
    return port;
}

typedef struct Component Component;
struct Component {
    char* name;
    ComponentPort *data_in[10];
    ComponentPort *data_out[10];
    ComponentPort *control_in;
    void * (*data_fn_pointer[10])(Component*, void*);
    void * (*control_fn_pointer)(Component*, void*);
    u64 data_in_size[10];
    u64 data_out_size[10];
    u64 control_size;
    u32 parallelism_level;
    pthread_t *threads;
    Arena *arena;
    void* extra_data;
};

b8 Port_data_out_push(Component *comp, u8 port_idx, void* data, u64 len);
b8 Port_push(ComponentPort* rb, void *data, u64 len);
u64 Port_pull(ComponentPort* rb, void *data, u64 len);

void* Component_run_thread(void* args);

void Component_start(Component* component) {
    printf("Starting component: %s with %d threads\n", component->name, component->parallelism_level);
    for (int i = 0; i < component->parallelism_level; ++i) {
        pthread_create(&component->threads[i], NULL, Component_run_thread, component);
    }
    printf("Component %s started.\n", component->name);
}

void* Component_wait_end(Component* component) {
    for (int i = 0; i < component->parallelism_level; ++i) {
        pthread_join(component->threads[i], NULL);
    }
    return NULL;
}

Component* Component_new(
    char* name,
    Arena *arena,
    void * (*control_fn_pointer)(Component*, void*),
    u64 data_control_size,
    u32 parallelism_level,
    void* extra_data
) {
    Component *comp = Arena_alloc(arena, sizeof(Component));
    comp->name = name;
    
    ComponentPort *control_port = Port_create(arena, data_control_size);
    comp->control_in = control_port;
    comp->control_size = data_control_size;
    comp->control_fn_pointer = (void*)control_fn_pointer;

    comp->parallelism_level = parallelism_level;
    comp->arena = arena;  // Store arena reference
    comp->threads = Arena_alloc(arena, sizeof(pthread_t) * parallelism_level);

    comp->extra_data = extra_data;
    return comp;
}

Component* Component_Flow(
    char* name,
    Arena *arena,
    void * (*data_fn_pointer)(Component*, void*),
    u64 data_in_size,
    u64 data_out_size,
    void * (*control_fn_pointer)(Component*, void*),
    u64 data_control_size,
    u32 parallelism_level,
    void* extra_data
) {
    Component *comp = Component_new(name, arena, control_fn_pointer, data_control_size, parallelism_level, extra_data);
    
    comp->data_in[0] = Port_create(arena, data_in_size);
    comp->data_in_size[0] = data_in_size;
    comp->data_fn_pointer[0] = (void*)data_fn_pointer;
    
    comp->data_out[0] = Port_create(arena, data_out_size);
    comp->data_out_size[0] = data_out_size;

    return comp;
}

Component* Component_Sink(
    char* name,
    Arena *arena,
    void * (*data_fn_pointer)(Component*, void*),
    u64 data_in_size,
    void * (*control_fn_pointer)(Component*, void*),
    u64 data_control_size,
    u32 parallelism_level,
    void* extra_data
) {
    Component *comp = Component_new(name, arena, control_fn_pointer, data_control_size, parallelism_level, extra_data);
    
    comp->data_in[0] = Port_create(arena, data_in_size);;
    comp->data_in_size[0] = data_in_size;
    comp->data_fn_pointer[0] = (void*)data_fn_pointer;

    return comp;
}

#define COMP_FLOW(Tin, Tout, Tcontrol, name, arena, data_fn_pointer, control_fn_pointer, parallelism_level, extra_data) \
    Component_Flow(name, arena, data_fn_pointer, sizeof(Tin), sizeof(Tout), control_fn_pointer, sizeof(Tcontrol), parallelism_level, extra_data)

#define COMP_SINK(Tin, Tcontrol, name, arena, data_fn_pointer, control_fn_pointer, parallelism_level, extra_data) \
    Component_Sink(name, arena, data_fn_pointer, sizeof(Tin), control_fn_pointer, sizeof(Tcontrol), parallelism_level, extra_data)


void Component_push_control(Component* component, void *data, u64 len) {
    for (u64 i = 0; i < component->parallelism_level; ++i) {
        Port_push(component->control_in, data, len);
    }
}

typedef enum MsgType {
    MSG_TYPE_SIGNAL,
    MSG_TYPE_DATA,
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
     if (debug) printf("[Component: %s, threadID: %lu] Got Value: %lld\n", comp->name, (unsigned long)pthread_self(), *(i64*)(msg->data));

    i64 new_value = *(i64*)(msg->data) + 1000;
    *(i64*)(msg->data) = new_value;

    /* Port_push(comp->data_out[0], (char*)msg, sizeof(Message)); */
    Port_data_out_push(comp, 0, &msg, sizeof(Message*));
    if (debug)  printf("[Component: %s, threadID: %lu] Produced Value: %lld\n", comp->name, (unsigned long)pthread_self(), new_value);
    return NULL;
}

void* consume_i64(Component *comp, Message** message) {
    Message *msg = *message;

    ComponentPort *wait_port = comp->extra_data;
    if (debug)  printf("[Component: %s, threadID: %lu] Consumed Value: %lld\n", comp->name, (unsigned long)pthread_self(), *(i64*)(msg->data));
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

void* process_domain_message(Component *comp, Message* msg) {
    DomainMessage *dm = msg->data;
    dm->i2 = dm->i1 + 1000;

    printf("Domain Message processed: i1=%lld, i2=%lld\n", dm->i1, dm->i2);
    return (void*)true;
    /* b8* ret = malloc(sizeof(b8)); */
    /* return (void*)ret; */
}

int main () {
    Arena *arena = Arena_create(1024 * 1024 * 1024);

    // New
    Component *c1 = COMP_FLOW(Message, b8, b8, "CD1", arena,
        (void *)process_domain_message,
        (void *)process_control,
        4,
        NULL);
    Component_start(c1);

    DomainMessage dm1 = {0};
    dm1.i1 = 42;
    Message m1 = {0};
    m1.msgType = MSG_TYPE_DATA;
    m1.data = &dm1;
    Port_push(c1->data_in[0], &m1, sizeof(Message));

    DomainMessage dm2 = {0};
    dm2.i1 = 1042;
    Message m2 = {0};
    m2.msgType = MSG_TYPE_DATA;
    m2.data = &dm2;
    Port_push(c1->data_in[0], &m2, sizeof(Message));

    DomainMessage dm3 = {0};
    dm3.i1 = 1044;
    Message m3 = {0};
    m3.msgType = MSG_TYPE_DATA;
    m3.data = &dm3;
    Port_push(c1->data_in[0], &m3, sizeof(Message));

    int i = 0;
    while(i < 3) {
        b8 ret;
        u64 out_bytes = Port_pull(c1->data_out[0], &ret, sizeof(b8));
        if (out_bytes > 0) {
            printf("Done\n");
            i++;
        } else {
            sleep(1);
        }
    }
    return 0;

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

    Message msg[NUM_DATA] = {0};
    i64 data[NUM_DATA] = {0};

    printf("Pushing data: Bytes: %lu\n", NUM_DATA * sizeof(Message));
    u32 done_received = 0;
    b8 done = false;

    for (i64 i = 0; i < NUM_DATA; ++i) {
        data[i] = i + 1;
        strcpy(&msg[i].msgTypeName, "i64");
        msg[i].msgType = MSG_TYPE_DATA;
        msg[i].data = data + i;
        void* ptr = msg + i;
        Port_push(comps[0]->data_in[0], &ptr, sizeof(Message*));
        u64 received = Port_pull(wait_port, &done, sizeof(b8));
        if (debug) printf("Wait received %llu bytes\n", received);
        if (received > 0) {
            done_received += received / sizeof(b8);
            if (debug) printf("Received done signal. Total done: %u/%u\n", done_received, NUM_DATA);
        }

        /* Port_data_out_push(comps[0], 0, (void*)&msgCtx[i], sizeof(Message)); */
        /* if (debug) printf("Pushed %lu size\n", sizeof(msgCtx[i])); */
        if (debug) printf("Pushed %lld, %lu size\n", *(i64*)((Message*)(msg[i]).data), sizeof(Message*));
    }


    printf("Waiting for all data to be processed...\n");
    while (done_received < NUM_DATA) {
        u64 received = Port_pull(wait_port, (void*)&done, sizeof(b8));
        if (debug) printf("Wait received %llu bytes\n", received);
        if (received > 0) {
            done_received += received / sizeof(b8);
            if (debug) printf("Received done signal. Total done: %u/%u\n", done_received, NUM_DATA);
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
    i64 data2[NUM_DATA];
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

    return 0;
}

b8 ring_buffer_add(ComponentPort* rb, char* data, u64 len) {
    if (rb->full) {
        return false;
    }

    if (len > rb->max - rb->head) {
        return false;
    }

    if (len == 0) {
        return false; // No space to add data
    }

    if (len > 0) {
        for (u64 i = 0; i < len; i++) {
            rb->buffer[rb->head] = data[i];
            rb->head = (rb->head + 1) % rb->max;
            if (rb->head == rb->tail) {
                rb->full = 1; // Buffer is now full
                break;
            }
        }
    }
    return true;
}

u64 ring_buffer_available(ComponentPort* rb) {
    return rb->full ? rb->max : (rb->head >= rb->tail ? rb->head - rb->tail : rb->max + rb->head - rb->tail);
}

u64 ring_buffer_get(ComponentPort* rb, char* data, u64 len) {

    if ((rb->head == rb->tail) && !rb->full) {
        return false; // Buffer is empty
    }
    u64 available = ring_buffer_available(rb);
    if (available == 0) {
        return 0; // No data to get
    }
    
    if (len > 0 && available < len) {
        return 0;
    } 
    /* data = &rb->buffer[rb->tail]; */

    if (len == 0) { len = available; }
    for (u64 i = 0; i < len; i++) {
        data[i] = rb->buffer[rb->tail];
        rb->tail = (rb->tail + 1) % rb->max;
        rb->full = 0; // Buffer is no longer full
    }
    return len;
}

// Deprecated, use Port_data_out_push instead where applicable
b8 Port_push(ComponentPort* port, void* data, u64 len) {
    /* printf("push 1\n"); */
    pthread_mutex_lock(port->mutex);
    /* printf("push 2\n"); */
    while (port->full) {
        if (debug) printf("Push wait: Port Full (Backpressure - If you see program stuck with this message, it means there is data pushed to a port that is not consumed by any other component.)\n");
        pthread_cond_wait(port->cond_not_full, port->mutex);
    }
    b8 result = ring_buffer_add(port, data, len);
    if (result) { pthread_cond_signal(port->cond_not_empty); }
    pthread_mutex_unlock(port->mutex);
    /* printf("push 3\n"); */
    if (!result) { 
        printf("[threadID: %lu] Buffer full, waiting to push data...\n", (unsigned long)pthread_self());
    }
    /* printf("push 4\n"); */
    return result;
}

// Duplicate of Port_push but with component context for better logging
b8 Port_data_out_push(Component *comp, u8 port_idx, void* data, u64 len) {
    /* printf("push 1\n"); */
    ComponentPort *port = comp->data_out[port_idx];
    pthread_mutex_lock(port->mutex);
    /* printf("push 2\n"); */
    while (port->full) {
        if (debug) printf("[Component: %s, threadID: %lu] Push wait: Port Full (Backpressure - If you see program stuck with this message, it means there is data pushed to a port that is not consumed by any other component.)\n", comp->name, (unsigned long)pthread_self());
        pthread_cond_wait(port->cond_not_full, port->mutex);
    }
    b8 result = ring_buffer_add(port, data, len);
    if (result) { pthread_cond_signal(port->cond_not_empty); }
    pthread_mutex_unlock(port->mutex);
    /* printf("push 3\n"); */
    if (!result) { 
        printf("[Component: %s, threadID: %lu] Buffer full, waiting to push data...\n", comp->name, (unsigned long)pthread_self());
    }
    /* printf("push 4\n"); */
    return result;
}

u64 Port_pull(ComponentPort* rb, void* data, u64 len_requested_in_bytes) {
    /* printf("pull 1\n"); */
    pthread_mutex_lock(rb->mutex);
    /* printf("pull 2\n"); */
    u64 len = ring_buffer_get(rb, data, len_requested_in_bytes);
    if (!rb->full) { pthread_cond_signal(rb->cond_not_full); }
    /* printf("pull 3 %d\n", result); */
    pthread_mutex_unlock(rb->mutex);
    /* printf("pull 4 %d\n", result); */
    return len;
}

void* Component_run_thread(void* args) {
    Component *comp = args;
    void* (*process_data)(Component*, void*) = comp->data_fn_pointer[0];
    printf("[Component: %s, threadID: %lu] Started\n", comp->name, (unsigned long)pthread_self());

    /* char* control_data = Arena_alloc(comp->arena, comp->control_size); */
    char* control_data = malloc(comp->control_size);
    char* data = malloc(comp->data_in[0]->max);
    while(true) {
        /* u64 len = Port_pull(comp->data_in[0], (char*)data, 0); // Will consume in this thread all existing msgs in buffer in order */
        u64 len = Port_pull(comp->data_in[0], data, comp->data_in_size[0]); // Will only take 1 msg from buffer if exists.
        if (len > 0) {
            if (debug) printf("[Component: %s, threadID: %lu] Pulled %lld bytes\n", comp->name, (unsigned long)pthread_self(), len);

            /* for( u64 i = 0; i <= ceil((len / comp->data_in_size[0]) / comp->parallelism_level); ++i) { */
            for( u64 i = 0; i < len / comp->data_in_size[0]; ++i) {
                if (debug) printf("[Component: %s, threadID: %lu] Processing idx %lld\n", comp->name, (unsigned long)pthread_self(), i);
                void* ret = process_data(comp, data + (i * comp->data_in_size[0]));
                if (ret != NULL) {
                    Port_data_out_push(comp, 0, &ret, comp->data_out_size[0]);
                }
            }
        }
        else {
            len = Port_pull(comp->control_in, control_data, comp->control_size);
            if (len > 0) {
                if (debug) printf("[Component: %s, threadID: %lu] Processing control data...\n", comp->name, (unsigned long)pthread_self());
                void* (*process_control)(Component*, void*) = comp->control_fn_pointer;
                process_control(comp, (void*)control_data);
            } else {
                struct timespec ts;
                int rc = 0;
                clock_gettime(CLOCK_REALTIME, &ts);
                /* ts.tv_sec += 1; */
                ts.tv_nsec += 100000000; // 100 ms

                pthread_mutex_lock(comp->data_in[0]->mutex);
                b8 data_buffer_empty = (comp->data_in[0]->head == comp->data_in[0]->tail) && !comp->data_in[0]->full;
                b8 control_buffer_empty = (comp->control_in->head == comp->control_in->tail) && !comp->control_in->full;
                if (data_buffer_empty) {
                    /* pthread_cond_wait(comp->data_in[0]->cond_empty, comp->data_in[0]->mutex); */
                    rc = pthread_cond_timedwait(comp->data_in[0]->cond_not_empty, comp->data_in[0]->mutex, &ts);
                }
                pthread_mutex_unlock(comp->data_in[0]->mutex);

                /* printf("[Component: %s, threadID: %lu] No data available, waiting...\n", comp->name, (unsigned long)pthread_self()); */
                /* usleep(1000); */
                /* sleep(1); */
            }
        }
    }
    return NULL;
}
