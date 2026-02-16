#pragma once
#include "cstream.h"

// Source and Sink component types inspired by Akka Streams

#define MAX_SOURCE_DATA 1000

// Source component types
typedef enum SourceType {
    SOURCE_SINGLE,      // Emits a single element
    SOURCE_TICK,        // Emits at regular intervals
    SOURCE_RANGE,       // Emits a range of numbers
    SOURCE_FROM_ARRAY,  // Emits elements from an array
    SOURCE_REPEAT,      // Repeats a value N times
    SOURCE_UNFOLD,      // Generates elements using a function
    SOURCE_EMPTY        // Emits no elements (completes immediately)
} SourceType;

// Sink component types
typedef enum SinkType {
    SINK_FOREACH,       // Applies side effect to each element
    SINK_FOLD,          // Reduces elements using accumulator
    SINK_HEAD,          // Takes only the first element
    SINK_LAST,          // Takes only the last element
    SINK_COLLECT,       // Collects all elements into array
    SINK_COUNT,         // Counts number of elements
    SINK_IGNORE         // Discards all elements
} SinkType;

// Source component data structures
typedef struct SourceConfig {
    SourceType type;
    union {
        struct {
            void* value;
            u64 value_size;
        } single;
        
        struct {
            void* value;
            u64 value_size;
            u64 interval_ms;
            u64 max_ticks;
            u64 current_tick;
            u64 last_emit_time;
        } tick;
        
        struct {
            i64 start;
            i64 end;
            i64 step;
            i64 current;
        } range;
        
        struct {
            void* array;
            u64 element_size;
            u64 count;
            u64 current_index;
        } from_array;
        
        struct {
            void* value;
            u64 value_size;
            u64 repeat_count;
            u64 current_repeat;
        } repeat;
        
        struct {
            void* (*generator)(void* state, bool* has_next);
            void* state;
            u64 element_size;
        } unfold;
    } config;
    bool is_complete;
    bool is_started;
} SourceConfig;

// Sink component data structures
typedef struct SinkConfig {
    SinkType type;
    union {
        struct {
            void (*effect)(void* element, void* context);
            void* context;
        } foreach;
        
        struct {
            void* (*fold_fn)(void* accumulator, void* element);
            void* accumulator;
            u64 accumulator_size;
        } fold;
        
        struct {
            void* result;
            u64 result_size;
            bool has_result;
        } head;
        
        struct {
            void* result;
            u64 result_size;
            bool has_result;
        } last;
        
        struct {
            void* array;
            u64 element_size;
            u64 capacity;
            u64 count;
        } collect;
        
        struct {
            u64 count;
        } count;
    } config;
    bool is_complete;
} SinkConfig;

// Source component factory functions
Component* Source_single(Arena* arena, void* value, u64 value_size, const char* name);
Component* Source_tick(Arena* arena, void* value, u64 value_size, u64 interval_ms, u64 max_ticks, const char* name);
Component* Source_range(Arena* arena, i64 start, i64 end, i64 step, const char* name);
Component* Source_from_array(Arena* arena, void* array, u64 element_size, u64 count, const char* name);
Component* Source_repeat(Arena* arena, void* value, u64 value_size, u64 repeat_count, const char* name);
Component* Source_unfold(Arena* arena, void* (*generator)(void* state, bool* has_next), void* state, u64 element_size, const char* name);
Component* Source_empty(Arena* arena, const char* name);

// Sink component factory functions
Component* Sink_foreach(Arena* arena, void (*effect)(void* element, void* context), void* context, u64 element_size, const char* name);
Component* Sink_fold(Arena* arena, void* (*fold_fn)(void* accumulator, void* element), void* initial_accumulator, u64 accumulator_size, u64 element_size, const char* name);
Component* Sink_head(Arena* arena, u64 element_size, const char* name);
Component* Sink_last(Arena* arena, u64 element_size, const char* name);
Component* Sink_collect(Arena* arena, u64 element_size, u64 capacity, const char* name);
Component* Sink_count(Arena* arena, u64 element_size, const char* name);
Component* Sink_ignore(Arena* arena, u64 element_size, const char* name);

// Helper functions to get results from sinks
void* Sink_get_result(Component* sink_component);
u64 Sink_get_count(Component* sink_component);
void* Sink_get_collected_array(Component* sink_component, u64* out_count);

#ifdef CSTREAM_SOURCES_IMPL

// Get current time in milliseconds
static u64 get_current_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (u64)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
}

// Source component control function
void* source_control(Component* comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { 
        SourceConfig* config = (SourceConfig*)comp->extra_data;
        config->is_complete = true;
        pthread_exit(NULL); 
    }
    return NULL;
}

// Source thread function - generates data based on source type
void* source_run_thread(void* args) {
    Component *comp = (Component*)args;
    SourceConfig* config = (SourceConfig*)comp->extra_data;
    
    printf("[Source: %s] Started\n", comp->name);
    
    char* control_data = (char*)malloc(comp->control_size);
    config->is_started = true;
    
    while (!config->is_complete) {
        bool should_emit = false;
        void* data_to_emit = NULL;
        u64 data_size = 0;
        
        switch (config->type) {
            case SOURCE_SINGLE: {
                if (!config->is_started) break;
                should_emit = true;
                data_to_emit = config->config.single.value;
                data_size = config->config.single.value_size;
                config->is_complete = true;  // Single emits once then completes
                break;
            }
            
            case SOURCE_TICK: {
                u64 current_time = get_current_time_ms();
                if (current_time - config->config.tick.last_emit_time >= config->config.tick.interval_ms) {
                    if (config->config.tick.current_tick < config->config.tick.max_ticks) {
                        should_emit = true;
                        data_to_emit = config->config.tick.value;
                        data_size = config->config.tick.value_size;
                        config->config.tick.current_tick++;
                        config->config.tick.last_emit_time = current_time;
                    } else {
                        config->is_complete = true;
                    }
                }
                break;
            }
            
            case SOURCE_RANGE: {
                if (config->config.range.current < config->config.range.end) {
                    should_emit = true;
                    data_to_emit = &config->config.range.current;
                    data_size = sizeof(i64);
                    config->config.range.current += config->config.range.step;
                } else {
                    config->is_complete = true;
                }
                break;
            }
            
            case SOURCE_FROM_ARRAY: {
                if (config->config.from_array.current_index < config->config.from_array.count) {
                    should_emit = true;
                    char* array_base = (char*)config->config.from_array.array;
                    data_to_emit = array_base + (config->config.from_array.current_index * config->config.from_array.element_size);
                    data_size = config->config.from_array.element_size;
                    config->config.from_array.current_index++;
                } else {
                    config->is_complete = true;
                }
                break;
            }
            
            case SOURCE_REPEAT: {
                if (config->config.repeat.current_repeat < config->config.repeat.repeat_count) {
                    should_emit = true;
                    data_to_emit = config->config.repeat.value;
                    data_size = config->config.repeat.value_size;
                    config->config.repeat.current_repeat++;
                } else {
                    config->is_complete = true;
                }
                break;
            }
            
            case SOURCE_UNFOLD: {
                bool has_next = false;
                void* generated = config->config.unfold.generator(config->config.unfold.state, &has_next);
                if (has_next && generated) {
                    should_emit = true;
                    data_to_emit = generated;
                    data_size = config->config.unfold.element_size;
                } else {
                    config->is_complete = true;
                }
                break;
            }
            
            case SOURCE_EMPTY: {
                config->is_complete = true;
                break;
            }
        }
        
        if (should_emit && data_to_emit) {
            Port_data_out_push(comp, 0, data_to_emit, data_size);
            if (DEBUG) printf("[Source: %s] Emitted data\n", comp->name);
        }
        
        // Check for control messages
        u64 len = Port_pull(comp->control_in, control_data, comp->control_size);
        if (len > 0) {
            if (DEBUG) printf("[Source: %s] Processing control data...\n", comp->name);
            void* (*process_control)(Component*, void*) = comp->control_fn_pointer;
            process_control(comp, (void*)control_data);
        }
        
        if (!config->is_complete) {
            usleep(1000);  // Small delay to prevent busy waiting
        }
    }
    
    printf("[Source: %s] Completed\n", comp->name);
    free(control_data);
    return NULL;
}

// Sink component control function
void* sink_control(Component* comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { 
        SinkConfig* config = (SinkConfig*)comp->extra_data;
        config->is_complete = true;
        pthread_exit(NULL); 
    }
    return NULL;
}

// Sink data processing function
void* sink_process_data(Component* comp, void* element) {
    SinkConfig* config = (SinkConfig*)comp->extra_data;
    
    if (config->is_complete) return NULL;
    
    switch (config->type) {
        case SINK_FOREACH: {
            config->config.foreach.effect(element, config->config.foreach.context);
            break;
        }
        
        case SINK_FOLD: {
            config->config.fold.accumulator = config->config.fold.fold_fn(
                config->config.fold.accumulator, element);
            break;
        }
        
        case SINK_HEAD: {
            if (!config->config.head.has_result) {
                memcpy(config->config.head.result, element, config->config.head.result_size);
                config->config.head.has_result = true;
                config->is_complete = true;
            }
            break;
        }
        
        case SINK_LAST: {
            memcpy(config->config.last.result, element, config->config.last.result_size);
            config->config.last.has_result = true;
            break;
        }
        
        case SINK_COLLECT: {
            if (config->config.collect.count < config->config.collect.capacity) {
                char* array_base = (char*)config->config.collect.array;
                char* target = array_base + (config->config.collect.count * config->config.collect.element_size);
                memcpy(target, element, config->config.collect.element_size);
                config->config.collect.count++;
            }
            break;
        }
        
        case SINK_COUNT: {
            config->config.count.count++;
            break;
        }
        
        case SINK_IGNORE: {
            // Do nothing - just discard the element
            break;
        }
    }
    
    return NULL;
}

// Source factory functions implementation

Component* Source_single(Arena* arena, void* value, u64 value_size, const char* name) {
    SourceConfig* config = (SourceConfig*)Arena_alloc(arena, sizeof(SourceConfig));
    config->type = SOURCE_SINGLE;
    config->config.single.value = Arena_alloc(arena, value_size);
    memcpy(config->config.single.value, value, value_size);
    config->config.single.value_size = value_size;
    config->is_complete = false;
    config->is_started = false;
    
    Component* comp = Component_new((char*)name, arena, source_control, sizeof(b8), 1, config);
    comp->data_out[0] = Port_create(arena, value_size);
    comp->data_out_size[0] = value_size;
    
    // Override the thread function to use source_run_thread
    comp->threads = (pthread_t*)Arena_alloc(arena, sizeof(pthread_t));
    pthread_create(&comp->threads[0], NULL, source_run_thread, comp);
    
    return comp;
}

Component* Source_tick(Arena* arena, void* value, u64 value_size, u64 interval_ms, u64 max_ticks, const char* name) {
    SourceConfig* config = (SourceConfig*)Arena_alloc(arena, sizeof(SourceConfig));
    config->type = SOURCE_TICK;
    config->config.tick.value = Arena_alloc(arena, value_size);
    memcpy(config->config.tick.value, value, value_size);
    config->config.tick.value_size = value_size;
    config->config.tick.interval_ms = interval_ms;
    config->config.tick.max_ticks = max_ticks;
    config->config.tick.current_tick = 0;
    config->config.tick.last_emit_time = get_current_time_ms();
    config->is_complete = false;
    config->is_started = false;
    
    Component* comp = Component_new((char*)name, arena, source_control, sizeof(b8), 1, config);
    comp->data_out[0] = Port_create(arena, value_size);
    comp->data_out_size[0] = value_size;
    
    comp->threads = (pthread_t*)Arena_alloc(arena, sizeof(pthread_t));
    pthread_create(&comp->threads[0], NULL, source_run_thread, comp);
    
    return comp;
}

Component* Source_range(Arena* arena, i64 start, i64 end, i64 step, const char* name) {
    SourceConfig* config = (SourceConfig*)Arena_alloc(arena, sizeof(SourceConfig));
    config->type = SOURCE_RANGE;
    config->config.range.start = start;
    config->config.range.end = end;
    config->config.range.step = step;
    config->config.range.current = start;
    config->is_complete = false;
    config->is_started = false;
    
    Component* comp = Component_new((char*)name, arena, source_control, sizeof(b8), 1, config);
    comp->data_out[0] = Port_create(arena, sizeof(i64));
    comp->data_out_size[0] = sizeof(i64);
    
    comp->threads = (pthread_t*)Arena_alloc(arena, sizeof(pthread_t));
    pthread_create(&comp->threads[0], NULL, source_run_thread, comp);
    
    return comp;
}

Component* Source_from_array(Arena* arena, void* array, u64 element_size, u64 count, const char* name) {
    SourceConfig* config = (SourceConfig*)Arena_alloc(arena, sizeof(SourceConfig));
    config->type = SOURCE_FROM_ARRAY;
    
    // Copy the array data into the arena
    u64 total_size = element_size * count;
    config->config.from_array.array = Arena_alloc(arena, total_size);
    memcpy(config->config.from_array.array, array, total_size);
    config->config.from_array.element_size = element_size;
    config->config.from_array.count = count;
    config->config.from_array.current_index = 0;
    config->is_complete = false;
    config->is_started = false;
    
    Component* comp = Component_new((char*)name, arena, source_control, sizeof(b8), 1, config);
    comp->data_out[0] = Port_create(arena, element_size);
    comp->data_out_size[0] = element_size;
    
    comp->threads = (pthread_t*)Arena_alloc(arena, sizeof(pthread_t));
    pthread_create(&comp->threads[0], NULL, source_run_thread, comp);
    
    return comp;
}

Component* Source_repeat(Arena* arena, void* value, u64 value_size, u64 repeat_count, const char* name) {
    SourceConfig* config = (SourceConfig*)Arena_alloc(arena, sizeof(SourceConfig));
    config->type = SOURCE_REPEAT;
    config->config.repeat.value = Arena_alloc(arena, value_size);
    memcpy(config->config.repeat.value, value, value_size);
    config->config.repeat.value_size = value_size;
    config->config.repeat.repeat_count = repeat_count;
    config->config.repeat.current_repeat = 0;
    config->is_complete = false;
    config->is_started = false;
    
    Component* comp = Component_new((char*)name, arena, source_control, sizeof(b8), 1, config);
    comp->data_out[0] = Port_create(arena, value_size);
    comp->data_out_size[0] = value_size;
    
    comp->threads = (pthread_t*)Arena_alloc(arena, sizeof(pthread_t));
    pthread_create(&comp->threads[0], NULL, source_run_thread, comp);
    
    return comp;
}

Component* Source_empty(Arena* arena, const char* name) {
    SourceConfig* config = (SourceConfig*)Arena_alloc(arena, sizeof(SourceConfig));
    config->type = SOURCE_EMPTY;
    config->is_complete = true;  // Empty source completes immediately
    config->is_started = false;
    
    Component* comp = Component_new((char*)name, arena, source_control, sizeof(b8), 1, config);
    comp->data_out[0] = Port_create(arena, sizeof(int));  // Dummy output
    comp->data_out_size[0] = sizeof(int);
    
    return comp;
}

// Sink factory functions implementation

Component* Sink_foreach(Arena* arena, void (*effect)(void* element, void* context), void* context, u64 element_size, const char* name) {
    SinkConfig* config = (SinkConfig*)Arena_alloc(arena, sizeof(SinkConfig));
    config->type = SINK_FOREACH;
    config->config.foreach.effect = effect;
    config->config.foreach.context = context;
    config->is_complete = false;
    
    Component* comp = COMP_SINK(void*, b8, name, arena, sink_process_data, sink_control, 1, config);
    comp->data_in_size[0] = element_size;
    
    return comp;
}

Component* Sink_fold(Arena* arena, void* (*fold_fn)(void* accumulator, void* element), void* initial_accumulator, u64 accumulator_size, u64 element_size, const char* name) {
    SinkConfig* config = (SinkConfig*)Arena_alloc(arena, sizeof(SinkConfig));
    config->type = SINK_FOLD;
    config->config.fold.fold_fn = fold_fn;
    config->config.fold.accumulator = Arena_alloc(arena, accumulator_size);
    memcpy(config->config.fold.accumulator, initial_accumulator, accumulator_size);
    config->config.fold.accumulator_size = accumulator_size;
    config->is_complete = false;
    
    Component* comp = COMP_SINK(void*, b8, name, arena, sink_process_data, sink_control, 1, config);
    comp->data_in_size[0] = element_size;
    
    return comp;
}

Component* Sink_head(Arena* arena, u64 element_size, const char* name) {
    SinkConfig* config = (SinkConfig*)Arena_alloc(arena, sizeof(SinkConfig));
    config->type = SINK_HEAD;
    config->config.head.result = Arena_alloc(arena, element_size);
    config->config.head.result_size = element_size;
    config->config.head.has_result = false;
    config->is_complete = false;
    
    Component* comp = COMP_SINK(void*, b8, name, arena, sink_process_data, sink_control, 1, config);
    comp->data_in_size[0] = element_size;
    
    return comp;
}

Component* Sink_count(Arena* arena, u64 element_size, const char* name) {
    SinkConfig* config = (SinkConfig*)Arena_alloc(arena, sizeof(SinkConfig));
    config->type = SINK_COUNT;
    config->config.count.count = 0;
    config->is_complete = false;
    
    Component* comp = COMP_SINK(void*, b8, name, arena, sink_process_data, sink_control, 1, config);
    comp->data_in_size[0] = element_size;
    
    return comp;
}

Component* Sink_ignore(Arena* arena, u64 element_size, const char* name) {
    SinkConfig* config = (SinkConfig*)Arena_alloc(arena, sizeof(SinkConfig));
    config->type = SINK_IGNORE;
    config->is_complete = false;
    
    Component* comp = COMP_SINK(void*, b8, name, arena, sink_process_data, sink_control, 1, config);
    comp->data_in_size[0] = element_size;
    
    return comp;
}

// Helper functions to get results from sinks

void* Sink_get_result(Component* sink_component) {
    SinkConfig* config = (SinkConfig*)sink_component->extra_data;
    
    switch (config->type) {
        case SINK_FOLD:
            return config->config.fold.accumulator;
        case SINK_HEAD:
            return config->config.head.has_result ? config->config.head.result : NULL;
        case SINK_LAST:
            return config->config.last.has_result ? config->config.last.result : NULL;
        default:
            return NULL;
    }
}

u64 Sink_get_count(Component* sink_component) {
    SinkConfig* config = (SinkConfig*)sink_component->extra_data;
    
    if (config->type == SINK_COUNT) {
        return config->config.count.count;
    }
    return 0;
}

void* Sink_get_collected_array(Component* sink_component, u64* out_count) {
    SinkConfig* config = (SinkConfig*)sink_component->extra_data;
    
    if (config->type == SINK_COLLECT) {
        *out_count = config->config.collect.count;
        return config->config.collect.array;
    }
    
    *out_count = 0;
    return NULL;
}

#endif // CSTREAM_SOURCES_IMPL