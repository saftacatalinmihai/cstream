// test_plugin_reload.c — demonstrates automatic plugin recompilation and hot-reload.
//
// The test writes two versions of a plugin source file programmatically, starts the
// file watcher, and verifies that the running component's behaviour changes after
// the source is updated — without any manual intervention.

#define CSTREAM_IMPLEMENTATION
#include "cstream.h"
#include "assert.h"

#define GRN "\x1B[32m"
#define RESET "\x1B[0m"

// Version 1: increments an i64 value by 1.
static const char* PLUGIN_SRC_V1 =
    "#define CSTREAM_IMPLEMENTATION\n"
    "#include \"cstream.h\"\n"
    "static void* plugin_process(Component* comp, i64* value) {\n"
    "    *value += 1;\n"
    "    Port_data_out_push(comp, 0, value, sizeof(i64));\n"
    "    return NULL;\n"
    "}\n"
    "static void* plugin_control(__attribute__((unused)) Component* comp, void* data) {\n"
    "    b8* stop = (b8*)data;\n"
    "    if (*stop) { pthread_exit(NULL); }\n"
    "    return NULL;\n"
    "}\n"
    "Component* plugin_create_component(Arena* arena) {\n"
    "    return Component_Flow(\"ReloadComp\", arena,\n"
    "        (void*(*)(Component*, void*))plugin_process,\n"
    "        sizeof(i64), sizeof(i64),\n"
    "        (void*(*)(Component*, void*))plugin_control,\n"
    "        sizeof(b8), 1, NULL);\n"
    "}\n";

// Version 2: increments an i64 value by 10 (behaviour changed).
static const char* PLUGIN_SRC_V2 =
    "#define CSTREAM_IMPLEMENTATION\n"
    "#include \"cstream.h\"\n"
    "static void* plugin_process(Component* comp, i64* value) {\n"
    "    *value += 10;\n"
    "    Port_data_out_push(comp, 0, value, sizeof(i64));\n"
    "    return NULL;\n"
    "}\n"
    "static void* plugin_control(__attribute__((unused)) Component* comp, void* data) {\n"
    "    b8* stop = (b8*)data;\n"
    "    if (*stop) { pthread_exit(NULL); }\n"
    "    return NULL;\n"
    "}\n"
    "Component* plugin_create_component(Arena* arena) {\n"
    "    return Component_Flow(\"ReloadComp\", arena,\n"
    "        (void*(*)(Component*, void*))plugin_process,\n"
    "        sizeof(i64), sizeof(i64),\n"
    "        (void*(*)(Component*, void*))plugin_control,\n"
    "        sizeof(b8), 1, NULL);\n"
    "}\n";

static void write_file(const char* path, const char* content) {
    FILE* f = fopen(path, "w");
    assert(f != NULL);
    fputs(content, f);
    fclose(f);
}

int main(void) {
    const char* src_file = "/tmp/cstream_reload_plugin.c";
    const char* so_file  = "/tmp/cstream_reload_plugin.so";
    // Get the directory containing cstream.h by reading /proc/self/exe or using __FILE__.
    // Simplest: use the directory of this source file at compile time.
    const char* inc_dir  = CSTREAM_HEADER_DIR;
    char compile_cmd[512];
    snprintf(compile_cmd, sizeof(compile_cmd),
             "cc -std=c11 -shared -fPIC -I%s -o %s %s -lpthread 2>/dev/null",
             inc_dir, so_file, src_file);

    // 1. Write version 1 and compile it.
    write_file(src_file, PLUGIN_SRC_V1);
    assert(system(compile_cmd) == 0);

    // 2. Load the initial plugin and create the component.
    Plugin* plugin = Plugin_load(so_file);
    assert(plugin != NULL);
    void* sym = Plugin_get_symbol(plugin, "plugin_create_component");
    assert(sym != NULL);
    PluginComponentFactory factory;
    memcpy(&factory, &sym, sizeof(factory));

    Arena* arena = Arena_create(4 * 1024 * 1024);
    Component* comp = factory(arena);
    assert(comp != NULL);

    ComponentPort* result_port = Port_create(arena, sizeof(i64));
    comp->data_out[0] = result_port;
    Component_start(comp);

    // 3. Start the file watcher — recompiles and reloads automatically on change.
    PluginWatcher* watcher = Plugin_watch_start(&plugin, comp, arena,
                                                src_file, compile_cmd,
                                                "plugin_create_component");
    assert(watcher != NULL);

    // 4. Verify version 1 behaviour: input 5 → output 6 (+1).
    i64 val = 5;
    Port_push(comp->data_in[0], &val, sizeof(i64));
    i64 out = 0;
    while (Port_pull(result_port, &out, sizeof(i64)) == 0) { usleep(1000); }
    printf("v1 result: %ld (expected 6)\n", out);
    assert(out == 6);

    // 5. Overwrite source with version 2 — watcher will auto-detect and reload.
    printf("Updating plugin source to v2 (+10), waiting for hot-reload...\n");
    write_file(src_file, PLUGIN_SRC_V2);

    // Poll until the watcher confirms at least one reload, with a 5-second timeout.
    int elapsed_us = 0;
    while (Plugin_watch_reload_count(watcher) == 0 && elapsed_us < 5000000) {
        usleep(50000);
        elapsed_us += 50000;
    }
    assert(Plugin_watch_reload_count(watcher) > 0);

    // 6. Verify version 2 behaviour: input 5 → output 15 (+10).
    val = 5;
    Port_push(comp->data_in[0], &val, sizeof(i64));
    out = 0;
    while (Port_pull(result_port, &out, sizeof(i64)) == 0) { usleep(1000); }
    printf("v2 result: %ld (expected 15)\n", out);
    assert(out == 15);

    // 7. Cleanup.
    Plugin_watch_stop(watcher);
    b8 stop = true;
    Component_push_control(comp, &stop, sizeof(b8));
    Component_wait_end(comp);

    Arena_destroy(arena);
    Plugin_unload(plugin);

    printf(GRN "> Plugin hot-reload test done.\n" RESET);
    return 0;
}
