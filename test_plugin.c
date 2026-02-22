// test_plugin.c — verifies that a Component can be loaded at runtime from a
// shared library plugin using Plugin_load / Plugin_get_symbol / Plugin_unload.

#define CSTREAM_IMPLEMENTATION
#include "cstream.h"
#include "assert.h"

#define GRN "\x1B[32m"
#define RESET "\x1B[0m"

int main(void) {
    // 1. Load the plugin shared library.
    Plugin* plugin = Plugin_load("./plugin_example.so");
    assert(plugin != NULL);

    // 2. Resolve the factory symbol.
    // POSIX requires dlsym to work for function pointers; use memcpy to avoid
    // the ISO C warning about object-pointer to function-pointer conversion.
    void* sym = Plugin_get_symbol(plugin, "plugin_create_component");
    assert(sym != NULL);
    PluginComponentFactory factory;
    memcpy(&factory, &sym, sizeof(factory));

    // 3. Create a component via the plugin factory.
    Arena* arena = Arena_create(1024 * 1024);
    Component* comp = factory(arena);
    assert(comp != NULL);

    // 4. Wire up a sink port so we can read results.
    ComponentPort* result_port = Port_create(arena, sizeof(i64));
    comp->data_out[0] = result_port;

    Component_start(comp);

    // 5. Push values and verify the plugin component increments each one by 1.
    i64 inputs[]   = {10, 20, 30};
    i64 expected[] = {11, 21, 31};
    int n = (int)(sizeof(inputs) / sizeof(inputs[0]));

    for (int i = 0; i < n; ++i) {
        Port_push(comp->data_in[0], &inputs[i], sizeof(i64));
    }

    int received = 0;
    while (received < n) {
        i64 out = 0;
        u64 bytes = Port_pull(result_port, &out, sizeof(i64));
        if (bytes > 0) {
            assert(out == expected[received]);
            received++;
        } else {
            usleep(1000);
        }
    }

    // 6. Stop the component.
    b8 stop = true;
    Component_push_control(comp, &stop, sizeof(b8));
    Component_wait_end(comp);

    Arena_destroy(arena);

    // 7. Unload the plugin.
    Plugin_unload(plugin);

    printf(GRN "> Plugin loading test done.\n" RESET);
    return 0;
}
