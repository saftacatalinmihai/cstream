// plugin_example.c — example cstream plugin loaded at runtime via dlopen.
// Compile to a shared library:
//   cc -std=c11 -shared -fPIC -o plugin_example.so plugin_example.c -lpthread
//
// The plugin exports a single symbol that matches the PluginComponentFactory
// typedef in cstream.h:
//
//   Component* plugin_create_component(Arena* arena);

#define CSTREAM_IMPLEMENTATION
#include "cstream.h"

static void* plugin_process(Component* comp, i64* value) {
    *value += 1;
    Port_data_out_push(comp, 0, value, sizeof(i64));
    return NULL;
}

static void* plugin_control(__attribute__((unused)) Component* comp, void* data) {
    b8* stop = (b8*)data;
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

// Required entry point — must match PluginComponentFactory signature.
Component* plugin_create_component(Arena* arena) {
    return Component_Flow(
        "PluginComp", arena,
        (void*(*)(Component*, void*))plugin_process,
        sizeof(i64), sizeof(i64),
        (void*(*)(Component*, void*))plugin_control,
        sizeof(b8),
        1,
        NULL
    );
}
