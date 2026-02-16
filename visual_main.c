#define CSTREAM_IMPL
#define CSTREAM_SOURCES_IMPL
#include "visual_cstream.h"

// Component function implementations (reused from main.c)
typedef struct DomainMessage {
    i64 i1;
    i64 i2;
    b8 b1;
    char str1[32];
} DomainMessage;

typedef struct Message {
    char msgTypeName[35];
    int msgType;
    void* data;
} Message;

void* process_message_c1(Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    dm->i2 = dm->i1 + 1;
    dm->i1 = dm->i1 + 1000;
    
    printf("C1 Domain Message processed: i1=%lld, i2=%lld\n", dm->i1, dm->i2);
    return msg;
}

void* process_message_c2(Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    dm->b1 = dm->i1 % 2 == 0;
    
    printf("C2 Domain Message processed: i1=%lld, b1=%d\n", dm->i1, dm->b1);
    return msg;
}

void* process_message_c3(Component *comp, Message** msg) {
    DomainMessage *dm = (*msg)->data;
    snprintf(dm->str1, sizeof(dm->str1), "Str Value: %lld", dm->i1);
    
    printf("C3 Domain Message processed: i1=%lld, str1=%s\n", dm->i1, dm->str1);
    return msg;
}

static void* process_control(Component *comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

Component* create_C1(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "C1", arena,
        (void *)process_message_c1,
        (void *)process_control,
        4,
        NULL);
}

Component* create_C2(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "C2", arena,
        (void *)process_message_c2,
        (void *)process_control,
        4,
        NULL);
}

Component* create_C3(Arena *arena) {
    return COMP_FLOW(Message*, Message*, b8, "C3", arena,
        (void *)process_message_c3,
        (void *)process_control,
        4,
        NULL);
}

int main() {
    // Initialize raylib
    const int screenWidth = 1200;
    const int screenHeight = 800;
    
    InitWindow(screenWidth, screenHeight, "CStream Visual Programming");
    SetTargetFPS(60);
    
    // Initialize cstream
    Arena *arena = Arena_create(1024 * 1024 * 1024);
    
    // Create components
    Component *c1 = create_C1(arena);
    Component *c2 = create_C2(arena);
    Component *c3 = create_C3(arena);
    
    // Start components
    Component_start(c1);
    Component_start(c2);
    Component_start(c3);
    
    // Create visual editor
    VisualEditor *editor = VisualEditor_create(arena);
    
    // Add components to visual editor
    int visual_c1 = VisualEditor_add_component(editor, c1, (Vector2){100, 200}, "C1 - Transform");
    int visual_c2 = VisualEditor_add_component(editor, c2, (Vector2){400, 200}, "C2 - Process");
    int visual_c3 = VisualEditor_add_component(editor, c3, (Vector2){700, 200}, "C3 - Format");
    
    // Create some initial connections to demonstrate the flow
    VisualEditor_add_connection(editor, visual_c1, 0, visual_c2, 0);
    VisualEditor_add_connection(editor, visual_c2, 0, visual_c3, 0);
    
    // Prepare test data
    DomainMessage dm1 = {.i1 = 42};
    DomainMessage dm2 = {.i1 = 43};
    DomainMessage dm3 = {.i1 = 44};
    
    Message m1 = {.data = &dm1};
    Message m2 = {.data = &dm2};
    Message m3 = {.data = &dm3};
    
    void* m1_ptr = &m1;
    void* m2_ptr = &m2;
    void* m3_ptr = &m3;
    
    bool data_sent = false;
    
    // Main program loop
    int frame_count = 0;
    while (!WindowShouldClose() && frame_count < 3600) { // Exit after 60 seconds at 60 FPS
        frame_count++;
        
        // Send some test data after a delay
        if (!data_sent && GetTime() > 2.0) {
            printf("Sending test data to components...\n");
            Port_push(c1->data_in[0], &m1_ptr, sizeof(Message*));
            Port_push(c1->data_in[0], &m2_ptr, sizeof(Message*));
            Port_push(c1->data_in[0], &m3_ptr, sizeof(Message*));
            data_sent = true;
            printf("Test data sent to pipeline\n");
        }
        
        // Check for processed output occasionally
        static int last_check = 0;
        if (frame_count - last_check > 60) { // Check every second
            Message* ret;
            u64 out_bytes = Port_pull(c3->data_out[0], &ret, sizeof(Message*));
            if (out_bytes > 0) {
                printf("Got processed output: %s\n", ((DomainMessage*)ret->data)->str1);
            }
            last_check = frame_count;
        }
        
        // Handle input and update visual editor
        VisualEditor_handle_input(editor);
        VisualEditor_update(editor);
        
        // Render everything
        VisualEditor_render(editor);
        
        // Draw instructions
        DrawText("Visual CStream Programming Interface", 10, 10, 20, WHITE);
        DrawText("• Drag components to move them around", 10, screenHeight - 80, 14, LIGHTGRAY);
        DrawText("• Click and drag from red output ports to blue input ports to connect", 10, screenHeight - 60, 14, LIGHTGRAY);
        DrawText("• Press D to toggle debug info, Delete to remove selected components", 10, screenHeight - 40, 14, LIGHTGRAY);
        DrawText("• Test data will be sent automatically after 2 seconds", 10, screenHeight - 20, 14, LIGHTGRAY);
    }
    
    // Cleanup
    printf("Shutting down visual editor...\n");
    
    // Stop components
    b8 stop = true;
    Component_push_control(c1, &stop, sizeof(b8));
    Component_push_control(c2, &stop, sizeof(b8));
    Component_push_control(c3, &stop, sizeof(b8));
    
    // Wait for components to finish
    Component_wait_end(c1);
    Component_wait_end(c2);
    Component_wait_end(c3);
    
    VisualEditor_destroy(editor);
    Arena_destroy(arena);
    
    CloseWindow();
    
    return 0;
}