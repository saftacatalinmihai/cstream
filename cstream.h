#ifndef CSTREAM_H
#define CSTREAM_H

#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <stdbool.h>
#include <unistd.h>
#include <string.h>

#define __CSTREAM_VERSION__ "0.0.4"

#define DEBUG false

typedef int8_t  i8;
typedef int16_t i16;
typedef int32_t i32;
typedef int64_t i64;

typedef uint8_t  u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef u8 b8;

#define MAX_PORTS 10
#define MAX_FN_POINTERS 10

typedef struct Arena {
    char* memory;
    u64 size;
    u64 offset;
} Arena;
Arena*  Arena_create(u64 size);
void*   Arena_alloc(Arena* arena, u64 size);
void    Arena_reset(Arena* arena);
void    Arena_destroy(Arena* arena);
void    Arena_print_memory(Arena* arena);

#define ARENA_ALLOC(arena, T) Arena_alloc(arena, sizeof(T))

typedef struct ComponentPort ComponentPort;
ComponentPort * Port_create(Arena *arena, u16 data_size);
b8  Port_push(ComponentPort* port, void *data, u64 len);
u64 Port_pull(ComponentPort* port, void *data, u64 len);

typedef struct Component Component;
b8   Component_push_data   (Component* component, u8 port_idx, void* data, u64 len);
void Component_push_control(Component* component,              void *data, u64 len);

void  Component_start(Component* component);
void* Component_wait_end(Component* component);

Component* Component_new(
    char* name,
    Arena *arena,
    void * (*control_fn_pointer)(Component*, void*),
    u64 data_control_size,
    u32 parallelism_level,
    void* extra_data
);
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
);
Component* Component_Flow_Map(
    Component *comp, 
    u8 port_idx, 
    void * (*data_fn_pointer)(Component*, void*)
);
Component* Component_Sink(
    char* name,
    Arena *arena,
    void * (*data_fn_pointer)(Component*, void*),
    u64 data_in_size,
    void * (*control_fn_pointer)(Component*, void*),
    u64 data_control_size,
    u32 parallelism_level,
    void* extra_data
);

#define COMP_FLOW(Tin, Tout, Tcontrol, name, arena, data_fn_pointer, control_fn_pointer, parallelism_level, extra_data) \
    Component_Flow(name, arena, data_fn_pointer, sizeof(Tin), sizeof(Tout), control_fn_pointer, sizeof(Tcontrol), parallelism_level, extra_data)

#define COMP_SINK(Tin, Tcontrol, name, arena, data_fn_pointer, control_fn_pointer, parallelism_level, extra_data) \
    Component_Sink(name, arena, data_fn_pointer, sizeof(Tin), control_fn_pointer, sizeof(Tcontrol), parallelism_level, extra_data)

#endif // CSTREAM_H

// #define CSTREAM_IMPLEMENTATION
#ifdef CSTREAM_IMPLEMENTATION
Arena* Arena_create(u64 size) {
    Arena* arena = (Arena*)malloc(sizeof(Arena));
    arena->memory = (char*)calloc(size, 1);
    /* arena->memory = malloc(size); */
    arena->size = size;
    arena->offset = 0;
    return arena;
}

void* Arena_alloc(Arena* arena, u64 size) {
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

struct ComponentPort { // A ring buffer for data
    char* buffer;
    u64 head;
    u64 tail;
    u64 max; //of the buffer
    b8 full;
    pthread_mutex_t *mutex;
    pthread_cond_t *cond_not_full;
    pthread_cond_t *cond_not_empty;
};

ComponentPort * Port_create(Arena *arena, u16 data_size) {
    ComponentPort *port = (ComponentPort*)Arena_alloc(arena, sizeof(ComponentPort));
    port->max = data_size * 4;
    port->buffer = (char*)Arena_alloc(arena, data_size * port->max);
    port->mutex = (pthread_mutex_t*) Arena_alloc(arena,sizeof(pthread_mutex_t));
    pthread_mutex_init(port->mutex, NULL);
    port->cond_not_full =  (pthread_cond_t*)Arena_alloc(arena, sizeof(pthread_cond_t));
    pthread_cond_init(port->cond_not_full, NULL);
    port->cond_not_empty =  (pthread_cond_t*)Arena_alloc(arena, sizeof(pthread_cond_t));
    pthread_cond_init(port->cond_not_empty, NULL);
    return port;
}

b8 Port_push(ComponentPort* port, void *data, u64 len) {
    pthread_mutex_lock(port->mutex);
    
    // Wait while buffer is full
    while (port->full) {
        pthread_cond_wait(port->cond_not_full, port->mutex);
    }
    
    // Copy data to buffer
    memcpy(port->buffer + port->tail, data, len);
    port->tail = (port->tail + len) % port->max;
    
    // Check if buffer is now full
    if (port->tail == port->head) {
        port->full = true;
    }
    
    // Signal that buffer is not empty
    pthread_cond_signal(port->cond_not_empty);
    pthread_mutex_unlock(port->mutex);
    
    return true;
}

u64 Port_pull(ComponentPort* port, void *data, u64 len) {
    pthread_mutex_lock(port->mutex);
    
    // Check if buffer is empty
    b8 is_empty = (port->head == port->tail) && !port->full;
    if (is_empty) {
        pthread_mutex_unlock(port->mutex);
        return 0;
    }
    
    // Calculate available data
    u64 available;
    if (port->full) {
        available = port->max;
    } else if (port->tail >= port->head) {
        available = port->tail - port->head;
    } else {
        available = port->max - port->head + port->tail;
    }
    
    // Don't read more than requested or available
    u64 to_read = (len < available) ? len : available;
    
    // Copy data from buffer
    if (port->head + to_read <= port->max) {
        // Simple case: no wraparound
        memcpy(data, port->buffer + port->head, to_read);
    } else {
        // Wraparound case
        u64 first_part = port->max - port->head;
        memcpy(data, port->buffer + port->head, first_part);
        memcpy((char*)data + first_part, port->buffer, to_read - first_part);
    }
    
    port->head = (port->head + to_read) % port->max;
    port->full = false;
    
    // Signal that buffer is not full
    pthread_cond_signal(port->cond_not_full);
    pthread_mutex_unlock(port->mutex);
    
    return to_read;
}

struct Component {
    char* name;
    ComponentPort *data_in[MAX_PORTS];
    ComponentPort *data_out[MAX_PORTS];
    ComponentPort *control_in;
    void * (*data_fn_pointer[MAX_PORTS][MAX_FN_POINTERS])(Component*, void*);
    void * (*control_fn_pointer)(Component*, void*);
    u64 data_in_size[MAX_PORTS];
    u64 data_out_size[MAX_PORTS];
    u64 control_size;
    u32 parallelism_level;
    pthread_t *threads;
    Arena *arena;
    void* extra_data;
};

b8 Port_data_out_push(Component *comp, u8 port_idx, void* data, u64 len) {
    if (port_idx >= MAX_PORTS || comp->data_out[port_idx] == NULL) {
        return false;
    }
    return Port_push(comp->data_out[port_idx], data, len);
}

void* Component_run_thread(void* args);

void Component_start(Component* component) {
    printf("Starting component: %s with %d threads\n", component->name, component->parallelism_level);
    for (u32 i = 0; i < component->parallelism_level; ++i) {
        pthread_create(&component->threads[i], NULL, Component_run_thread, component);
    }
    printf("Component %s started.\n", component->name);
}

void* Component_wait_end(Component* component) {
    for (u32 i = 0; i < component->parallelism_level; ++i) {
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
    void* extra_data)
{
    Component *comp = (Component*)Arena_alloc(arena, sizeof(Component));
    comp->name = name;
    
    ComponentPort *control_port = Port_create(arena, data_control_size);
    comp->control_in = control_port;
    comp->control_size = data_control_size;
    comp->control_fn_pointer = (void * (*)(Component*, void*))control_fn_pointer;

    comp->parallelism_level = parallelism_level;
    comp->arena = arena;  // Store arena reference
    comp->threads = (pthread_t*)Arena_alloc(arena, sizeof(pthread_t) * parallelism_level);

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
    comp->data_fn_pointer[0][0] = (void * (*)(Component*, void*))data_fn_pointer;
    
    comp->data_out[0] = Port_create(arena, data_out_size);
    comp->data_out_size[0] = data_out_size;

    return comp;
}

Component* Component_Flow_Map(Component *comp, u8 port_idx, void * (*data_fn_pointer)(Component*, void*))  {
    for (int i = 1; i < MAX_FN_POINTERS; ++i) {
        if (comp->data_fn_pointer[port_idx][i] != NULL) { continue; }
        comp->data_fn_pointer[port_idx][i] = (void * (*)(Component*, void*))data_fn_pointer;
        return comp;
    }
    fprintf(stderr, "Error: Maximum number of ports reached for component %s.\n", comp->name);
    return NULL;
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
    comp->data_fn_pointer[0][0] = (void * (*)(Component*, void*))data_fn_pointer;

    return comp;
}

void Component_push_control(Component* component, void *data, u64 len) {
    for (u64 i = 0; i < component->parallelism_level; ++i) {
        Port_push(component->control_in, data, len);
    }
}

void* Component_run_thread(void* args) {
    Component *comp = (Component*)args;
    void* (**process_data)(Component*, void*) = comp->data_fn_pointer[0];
    printf("[Component: %s, threadID: %lu] Started\n", comp->name, (unsigned long)pthread_self());

    /* char* control_data = Arena_alloc(comp->arena, comp->control_size); */
    char* control_data = (char*)malloc(comp->control_size);
    char* data = (char*)malloc(comp->data_in[0]->max);
    while(true) {
        /* u64 len = Port_pull(comp->data_in[0], (char*)data, 0); // Will consume in this thread all existing msgs in buffer in order */
        u64 len = Port_pull(comp->data_in[0], data, comp->data_in_size[0]); // Will only take 1 msg from buffer if exists.
        if (len > 0) {
            if (DEBUG) printf("[Component: %s, threadID: %lu] Pulled %lld bytes\n", comp->name, (unsigned long)pthread_self(), len);

            /* for( u64 i = 0; i <= ceil((len / comp->data_in_size[0]) / comp->parallelism_level); ++i) { */
            for( u64 i = 0; i < len / comp->data_in_size[0]; ++i) {
                if (DEBUG) printf("[Component: %s, threadID: %lu] Processing idx %lld\n", comp->name, (unsigned long)pthread_self(), i);

                int j = 0;
                if (process_data[j] == NULL) { break; }
                void* ret = process_data[j](comp, data + (i * comp->data_in_size[0]));
                while (ret != NULL && j < MAX_FN_POINTERS) {
                    if (process_data[j+1] == NULL) {
                        if (DEBUG) printf("[Component: %s, threadID: %lu] Processing done, pushing to output...\n", comp->name, (unsigned long)pthread_self());
                        Port_data_out_push(comp, 0, ret, comp->data_out_size[0]);
                        break;
                    } else {
                        ret = process_data[j+1](comp, ret);
                        j++;
                    }
                }
                
            }
        }
        else {
            len = Port_pull(comp->control_in, control_data, comp->control_size);
            if (len > 0) {
                if (DEBUG) printf("[Component: %s, threadID: %lu] Processing control data...\n", comp->name, (unsigned long)pthread_self());
                void* (*process_control)(Component*, void*) = comp->control_fn_pointer;
                process_control(comp, (void*)control_data);
            } else {
                struct timespec ts;
                // int rc = 0;
                clock_gettime(CLOCK_REALTIME, &ts);
                /* ts.tv_sec += 1; */
                ts.tv_nsec += 100000000; // 100 ms

                pthread_mutex_lock(comp->data_in[0]->mutex);
                b8 data_buffer_empty = (comp->data_in[0]->head == comp->data_in[0]->tail) && !comp->data_in[0]->full;
                // b8 control_buffer_empty = (comp->control_in->head == comp->control_in->tail) && !comp->control_in->full;
                if (data_buffer_empty) {
                    /* pthread_cond_wait(comp->data_in[0]->cond_empty, comp->data_in[0]->mutex); */
                    pthread_cond_timedwait(comp->data_in[0]->cond_not_empty, comp->data_in[0]->mutex, &ts);
                    // rc = pthread_cond_timedwait(comp->data_in[0]->cond_not_empty, comp->data_in[0]->mutex, &ts);
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

#endif // CSTREAM_IMPLEMENTATION
