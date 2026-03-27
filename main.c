#include <_time.h>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <math.h>

typedef int8_t  i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

bool debug = false;
/* bool debug = true; */

// /Arena\

typedef struct Arena {
    char* memory;
    u64 size;
    u64 offset;
    u64 total_accesses;        // For heat map
    u64 start_timestamp;       // For timing
} Arena;

void* Arena_alloc_with_type(Arena* arena, u64 size, char* custom_name);

#define ARENA_ALLOC(arena, T) Arena_alloc_with_type(arena, sizeof(T), TYPE_##T, #T)
#define ARENA_ALLOC_NAMED(arena, T, name) Arena_alloc_with_type(arena, sizeof(T), TYPE_##T, name)

Arena* Arena_create(u64 size) {
    Arena* arena = malloc(sizeof(Arena));
    arena->memory = malloc(size);
    arena->size = size;
    arena->offset = 0;
    arena->total_accesses = 0;
    arena->start_timestamp = time(NULL);
    return arena;
}

void* Arena_alloc(Arena* arena, u64 size) {
    return Arena_alloc_with_type(arena, size, NULL);
}

void* Arena_alloc_with_type(Arena* arena, u64 size, char* custom_name) {
    if (arena->offset + size > arena->size) {
        return NULL; // Not enough memory
    }
    
    void* ptr = arena->memory + arena->offset;
    arena->offset += size;
    return ptr;
}

void Arena_reset(Arena* arena) {
    arena->offset = 0;
}

void Arena_destroy(Arena* arena) {
    // Clean up allocation tracking
    free(arena->memory);
    free(arena);
}


void Arena_print_memory(Arena* arena) {
    for (u64 i = 0; i < arena->offset; i++) {
        printf("%02x ", (unsigned char)arena->memory[i]);
    }
    printf("\n");
}

// \Arena/ 
typedef struct ComponentPort { // A ring buffer for data
    char* buffer;
    u64 head;
    u64 tail;
    u64 max; //of the buffer
    bool full;
    pthread_mutex_t *mutex;
    pthread_cond_t *cond_full;
    pthread_cond_t *cond_empty;
} ComponentPort;

ComponentPort * Port_create(Arena *arena, u16 data_size) {
    ComponentPort *port = Arena_alloc(arena, sizeof(ComponentPort));
    port->max = data_size * 16;
    port->buffer = Arena_alloc(arena, data_size * port->max);
    port->mutex = (pthread_mutex_t*) Arena_alloc(arena,sizeof(pthread_mutex_t));
    pthread_mutex_init(port->mutex, NULL);
    port->cond_full =  (pthread_cond_t*)Arena_alloc(arena, sizeof(pthread_cond_t));
    pthread_cond_init(port->cond_full, NULL);
    port->cond_empty =  (pthread_cond_t*)Arena_alloc(arena, sizeof(pthread_cond_t));
    pthread_cond_init(port->cond_empty, NULL);
    return port;
}

bool Port_push(ComponentPort* rb, char *data, u64 len);
u64 Port_pull(ComponentPort* rb, char *data, u64 len);

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
    Arena *arena;  // Add reference to arena for access tracking
};

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
    u32 parallelism_level
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
    u32 parallelism_level
) {
    Component *comp = Component_new(name, arena, control_fn_pointer, data_control_size, parallelism_level);
    
    comp->data_in[0] = Port_create(arena, data_in_size);;
    comp->data_in_size[0] = data_in_size;
    comp->data_fn_pointer[0] = (void*)data_fn_pointer;
    
    comp->data_out[0] = Port_create(arena, data_out_size);;;
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
    u32 parallelism_level
) {
    Component *comp = Component_new(name, arena, control_fn_pointer, data_control_size, parallelism_level);
    
    comp->data_in[0] = Port_create(arena, data_in_size);;
    comp->data_in_size[0] = data_in_size;
    comp->data_fn_pointer[0] = (void*)data_fn_pointer;

    return comp;
}

#define COMP_FLOW(Tin, Tout, Tcontrol, name, arena, data_fn_pointer, control_fn_pointer, parallelism_level) \
    Component_Flow(name, arena, data_fn_pointer, sizeof(Tin), sizeof(Tout), control_fn_pointer, sizeof(Tcontrol), parallelism_level)

#define COMP_SINK(Tin, Tcontrol, name, arena, data_fn_pointer, control_fn_pointer, parallelism_level) \
    Component_Sink(name, arena, data_fn_pointer, sizeof(Tin), control_fn_pointer, sizeof(Tcontrol), parallelism_level)


void Component_push_control(Component* component, char *data, u64 len) {
    for (u64 i = 0; i < component->parallelism_level; ++i) {
        Port_push(component->control_in, data, len);
    }
}

typedef enum MsgType {
    MSG_TYPE_DATA,
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
    char* data;
} Message;

void* process_i64(Component *comp, Message* msg) {

    if ((i64)msg->data == -1) {
        printf("-1\n");
    }
    // usleep(1000000);
    usleep(100);
     if (debug) printf("[Component: %s, threadID: %d] Value: %lld\n", comp->name, pthread_self(), (i64)msg->data);

    i64 new_value = (i64)msg->data + 1000;
    msg->data = (char*)new_value;

    Port_push(comp->data_out[0], (char*)msg, sizeof(Message));
    if (debug)  printf("[Component: %s, threadID: %d] Produced Value: %lld\n", comp->name, pthread_self(), new_value);
    return NULL;
}

void* consume_i64(Component *comp, Message* msg) {
    if (debug)  printf("[Component: %s, threadID: %d] Consumed Value: %lld\n", comp->name, pthread_self(), (i64)msg->data);
    // sleep(2);
    return NULL;
}

void* process_control(Component *comp, void* control_data) {
    bool *stop = (bool*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

#define MB (1024 * 1024)
#define GB (1024 * 1024 * 1024)

#define NUM_COMPS 8


int main () {
    Arena *arena = Arena_create(1024 * 1024 * 1024);

    struct timespec start, t1, t2, t3, end;

    clock_gettime(CLOCK_MONOTONIC, &start);


    Component *comps[NUM_COMPS] = {0};
    char names[NUM_COMPS][4] = {0};
    for (int i = 0; i < NUM_COMPS; i++) {
        sprintf(names[i], "C%d", i + 1);
        Component *comp = COMP_FLOW(
            Message,
            Message,
            bool,
            names[i], arena,
            (void *)process_i64,
            (void *)process_control,
            4);
        comps[i] = comp;
        if (i > 0) {
            comps[i]->data_in[0] = comps[i - 1]->data_out[0];
        }
    }

    Component *comp_sink = COMP_SINK(
        Message, bool,
        "CS",
        arena,
        (void*)consume_i64,
        (void*)process_control, 
        4
    );

    comp_sink->data_in[0] = comps[NUM_COMPS-1]->data_out[0];

    for (int i = 0; i < NUM_COMPS; ++i) { Component_start(comps[i]); }
    Component_start(comp_sink);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    u32 num_data = 100042;
    Message *msgCtx = Arena_alloc(arena, sizeof(Message) * num_data);
    i64 *data = Arena_alloc(arena, sizeof(i64) * num_data);

    printf("Pushing data...\n");
    for (i64 i = 0; i < num_data; ++i) {
        data[i] = i + 1;
        msgCtx[i].data = (char*)data[i];
        msgCtx[i].msgType = MSG_TYPE_SIGNAL;
        strcpy(msgCtx[i].msgTypeName, "i64");
        Port_push(comps[0]->data_in[0], (char*)&msgCtx[i], sizeof(Message));
        if (debug) printf("Pushed %lu size\n", sizeof(msgCtx[i]));
        if (debug) printf("Pushed %lld\n", (i64)msgCtx[i].data);
    }

    clock_gettime(CLOCK_MONOTONIC, &t2);

    sleep(1);
    printf("Sending stop control ...\n");

    bool stop = true;

    for (int i=0; i < NUM_COMPS; i++) { Component_push_control(comps[i], (char *)&stop, sizeof(bool)); }
    Component_push_control(comp_sink, (char *)&stop, sizeof(bool));

    printf("Stop signals control.\n");

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
    i64 data2[num_data];
    for (i64 i = 0; i < num_data; ++i) {
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

bool ring_buffer_add(ComponentPort* rb, char* data, u64 len) {
    if (rb->full) {
        return false; // Buffer is full
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

bool Port_push(ComponentPort* rb, char* data, u64 len) {
    /* printf("push 1\n"); */
    pthread_mutex_lock(rb->mutex);
    /* printf("push 2\n"); */
    while (rb->full) {
        if (debug) printf("Push wait: Port Full (Backpressure - If you see program stuck with this message, it means there is data pushed to a port that is not consumed by any other component.)\n");
        pthread_cond_wait(rb->cond_full, rb->mutex);
    }
    bool result = ring_buffer_add(rb, data, len);
    if (result) { pthread_cond_signal(rb->cond_empty); }
    /* pthread_cond_signal(rb->cond); */
    pthread_mutex_unlock(rb->mutex);
    /* printf("push 3\n"); */
    if (!result) { 
        printf("[threadID: %d] Buffer full, waiting to push data...\n", pthread_self());
    }
    /* printf("push 4\n"); */
    return result;
}

u64 Port_pull(ComponentPort* rb, char* data, u64 len_requested_in_bytes) {
    /* printf("pull 1\n"); */
    pthread_mutex_lock(rb->mutex);
    /* printf("pull 2\n"); */
    u64 len = ring_buffer_get(rb, data, len_requested_in_bytes);
    if (!rb->full) { pthread_cond_signal(rb->cond_full); }
    /* printf("pull 3 %d\n", result); */
    pthread_mutex_unlock(rb->mutex);
    /* printf("pull 4 %d\n", result); */
    return len;
}

void* Component_run_thread(void* args) {
    Component *comp = args;
    void* (*process_data)(Component*, void*) = comp->data_fn_pointer[0];

    char* control_data = Arena_alloc(comp->arena, comp->control_size);
    char* data = malloc(comp->data_in[0]->max);
    while(true) {
        /* u64 len = Port_pull(comp->data_in[0], (char*)data, 0); // Will consume in this thread all existing msgs in buffer in order */
         u64 len = Port_pull(comp->data_in[0], (char*)data, comp->data_in_size[0]); // Will only take 1 msg from buffer if exists.
        if (len > 0) {
            if (debug) printf("[Component: %s, threadID: %d] Pulled %lld bytes\n", comp->name, pthread_self(), len);

            /* Message* msg = (Message*)data; */
            /* if  (msg->msgType == MSG_TYPE_SIGNAL) { */
            /*     if (debug) printf("[Component: %s, threadID: %d] Processing SIGNAL message...\n", comp->name, pthread_self()); */
            /*     SignalType signalType = ((SignalMessage*)msg->data)->signalType; */
            /*     switch (signalType) { */
            /*         case SIGNAL_TYPE_STOP: */
            /*             if (debug) printf("[Component: %s, threadID: %d] Received STOP signal. Exiting thread...\n", comp->name, pthread_self()); */
            /* //Should we send the signal forward to the next component in the chain ? */
            /*             pthread_exit(NULL); */
            /*             break; */
            /*         default: */
            /*             if (debug) printf("[Component: %s, threadID: %d] Unknown signal type received.\n", comp->name, pthread_self()); */
            /*             break; */
            /*     } */
            /*     continue; */
            /* } */
            /**/

            // for( u64 i = 0; i <= ceil((len / comp->data_in_size[0]) / comp->parallelism_level); ++i) {
            for( u64 i = 0; i < len / comp->data_in_size[0]; ++i) {
                // printf("[Component: %s, threadID: %d] Processing %lld\n", comp->name, pthread_self(), i);
                process_data(comp, (void*)data + (i * comp->data_in_size[0]));
            }
        }
        else {
            u64 len = Port_pull(comp->control_in, (char*)control_data, comp->control_size);
            if (len > 0) {
                if (debug) printf("[Component: %s, threadID: %d] Processing control data...\n", comp->name, pthread_self());
                void* (*process_control)(Component*, void*) = comp->control_fn_pointer;
                process_control(comp, (void*)control_data);
            } else {
                struct timespec ts;
                int rc = 0;
                clock_gettime(CLOCK_REALTIME, &ts);
                ts.tv_sec += 5;

                pthread_mutex_lock(comp->data_in[0]->mutex);
                bool data_buffer_empty = (comp->data_in[0]->head == comp->data_in[0]->tail) && !comp->data_in[0]->full;
                bool control_buffer_empty = (comp->control_in->head == comp->control_in->tail) && !comp->control_in->full;
                if (data_buffer_empty) {
                    // pthread_cond_wait(comp->data_in[0]->cond_empty, comp->data_in[0]->mutex);
                    rc = pthread_cond_timedwait(comp->data_in[0]->cond_empty, comp->data_in[0]->mutex, &ts);
                }
                pthread_mutex_unlock(comp->data_in[0]->mutex);

                /* printf("[Component: %s, threadID: %d] No data available, waiting...\n", comp->name, pthread_self()); */
            }
        }
    }
    return NULL;
}
