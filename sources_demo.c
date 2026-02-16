#define CSTREAM_IMPL
#define CSTREAM_SOURCES_IMPL
#include "visual_cstream.h"

// Sample data structures for demonstration
typedef struct NumberData {
    i64 value;
} NumberData;

typedef struct StringData {
    char text[64];
} StringData;

// Sample effect functions for sinks
static void print_number_effect(void* element, void* context) {
    NumberData* num = (NumberData*)element;
    printf("Sink received number: %lld\n", num->value);
}

static void print_string_effect(void* element, void* context) {
    StringData* str = (StringData*)element;
    printf("Sink received string: %s\n", str->text);
}

// Fold function for sum
static void* sum_fold(void* accumulator, void* element) {
    NumberData* acc = (NumberData*)accumulator;
    NumberData* num = (NumberData*)element;
    acc->value += num->value;
    return acc;
}

// Transform function for processing
static void* multiply_by_2(Component* comp, NumberData** data) {
    NumberData* input = *data;
    static NumberData output;
    output.value = input->value * 2;
    return &output;
}

// Transform function to convert number to string
static void* number_to_string(Component* comp, NumberData** data) {
    NumberData* input = *data;
    static StringData output;
    snprintf(output.text, sizeof(output.text), "Number: %lld", input->value);
    return &output;
}

static void* process_control(Component* comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

int main() {
    // Initialize raylib
    const int screenWidth = 1400;
    const int screenHeight = 900;
    
    InitWindow(screenWidth, screenHeight, "CStream Sources & Sinks Demo");
    SetTargetFPS(60);
    
    // Initialize cstream
    Arena *arena = Arena_create(1024 * 1024 * 1024);
    
    // Create visual editor
    VisualEditor *editor = VisualEditor_create(arena);
    
    // === SOURCES ===
    
    // Source 1: Single value source
    NumberData single_value = {42};
    Component *source_single = Source_single(arena, &single_value, sizeof(NumberData), "Single Source");
    int visual_single = VisualEditor_add_source_component(editor, source_single, 
        (Vector2){50, 100}, "Single(42)", SOURCE_SINGLE);
    
    // Source 2: Tick source (emits every 1000ms, 5 times)
    NumberData tick_value = {10};
    Component *source_tick = Source_tick(arena, &tick_value, sizeof(NumberData), 1000, 5, "Tick Source");
    int visual_tick = VisualEditor_add_source_component(editor, source_tick, 
        (Vector2){50, 250}, "Tick(10, 1s)", SOURCE_TICK);
    
    // Source 3: Range source (1 to 5)
    Component *source_range = Source_range(arena, 1, 6, 1, "Range Source");
    int visual_range = VisualEditor_add_source_component(editor, source_range, 
        (Vector2){50, 400}, "Range(1..5)", SOURCE_RANGE);
    
    // Source 4: Array source
    i64 numbers[] = {100, 200, 300, 400, 500};
    Component *source_array = Source_from_array(arena, numbers, sizeof(i64), 5, "Array Source");
    int visual_array = VisualEditor_add_source_component(editor, source_array, 
        (Vector2){50, 550}, "Array[5]", SOURCE_FROM_ARRAY);
    
    // === FLOW COMPONENTS ===
    
    // Transform: Multiply by 2
    Component *multiply_comp = COMP_FLOW(NumberData*, NumberData*, b8, "Multiply x2", arena,
        (void*)multiply_by_2, (void*)process_control, 1, NULL);
    int visual_multiply = VisualEditor_add_component(editor, multiply_comp, (Vector2){300, 150}, "Multiply x2");
    
    // Transform: Number to String
    Component *to_string_comp = COMP_FLOW(NumberData*, StringData*, b8, "To String", arena,
        (void*)number_to_string, (void*)process_control, 1, NULL);
    int visual_to_string = VisualEditor_add_component(editor, to_string_comp, (Vector2){600, 200}, "To String");
    
    // === SINKS ===
    
    // Sink 1: Print numbers
    Component *sink_print_num = Sink_foreach(arena, print_number_effect, NULL, sizeof(NumberData), "Print Numbers");
    int visual_sink_print = VisualEditor_add_sink_component(editor, sink_print_num, 
        (Vector2){550, 100}, "Print Numbers", SINK_FOREACH);
    
    // Sink 2: Sum (fold)
    NumberData sum_initial = {0};
    Component *sink_sum = Sink_fold(arena, sum_fold, &sum_initial, sizeof(NumberData), sizeof(NumberData), "Sum Sink");
    int visual_sink_sum = VisualEditor_add_sink_component(editor, sink_sum, 
        (Vector2){550, 300}, "Sum", SINK_FOLD);
    
    // Sink 3: Print strings
    Component *sink_print_str = Sink_foreach(arena, print_string_effect, NULL, sizeof(StringData), "Print Strings");
    int visual_sink_strings = VisualEditor_add_sink_component(editor, sink_print_str, 
        (Vector2){900, 200}, "Print Strings", SINK_FOREACH);
    
    // Sink 4: Count elements
    Component *sink_count = Sink_count(arena, sizeof(NumberData), "Count Sink");
    int visual_sink_count = VisualEditor_add_sink_component(editor, sink_count, 
        (Vector2){550, 450}, "Count", SINK_COUNT);
    
    // Sink 5: Head (first element)
    Component *sink_head = Sink_head(arena, sizeof(NumberData), "Head Sink");
    int visual_sink_head = VisualEditor_add_sink_component(editor, sink_head, 
        (Vector2){550, 600}, "Head", SINK_HEAD);
    
    // === CREATE CONNECTIONS ===
    
    // Single -> Print Numbers
    VisualEditor_add_connection(editor, visual_single, 0, visual_sink_print, 0);
    
    // Tick -> Multiply -> Sum
    VisualEditor_add_connection(editor, visual_tick, 0, visual_multiply, 0);
    VisualEditor_add_connection(editor, visual_multiply, 0, visual_sink_sum, 0);
    
    // Range -> Count
    VisualEditor_add_connection(editor, visual_range, 0, visual_sink_count, 0);
    
    // Array -> Head
    VisualEditor_add_connection(editor, visual_array, 0, visual_sink_head, 0);
    
    // Range -> To String -> Print Strings
    VisualEditor_add_connection(editor, visual_range, 0, visual_to_string, 0);
    VisualEditor_add_connection(editor, visual_to_string, 0, visual_sink_strings, 0);
    
    // Start all components
    Component_start(source_single);
    Component_start(source_tick);
    Component_start(source_range);
    Component_start(source_array);
    Component_start(multiply_comp);
    Component_start(to_string_comp);
    Component_start(sink_print_num);
    Component_start(sink_sum);
    Component_start(sink_print_str);
    Component_start(sink_count);
    Component_start(sink_head);
    
    printf("Starting CStream Sources & Sinks Demo\n");
    printf("======================================\n");
    
    // Main program loop
    int frame_count = 0;
    bool results_printed = false;
    
    while (!WindowShouldClose() && frame_count < 7200) { // Exit after 2 minutes at 60 FPS
        frame_count++;
        
        // Print results after 10 seconds
        if (!results_printed && frame_count > 600) {
            printf("\n=== SINK RESULTS ===\n");
            
            // Get sum result
            NumberData* sum_result = (NumberData*)Sink_get_result(sink_sum);
            if (sum_result) {
                printf("Sum result: %lld\n", sum_result->value);
            }
            
            // Get count result
            u64 count_result = Sink_get_count(sink_count);
            printf("Count result: %llu\n", count_result);
            
            // Get head result
            NumberData* head_result = (NumberData*)Sink_get_result(sink_head);
            if (head_result) {
                printf("Head result: %lld\n", head_result->value);
            }
            
            results_printed = true;
        }
        
        // Handle input and update visual editor
        VisualEditor_handle_input(editor);
        VisualEditor_update(editor);
        
        // Render everything
        VisualEditor_render(editor);
        
        // Draw instructions
        DrawText("CStream Sources & Sinks Visual Demo", 10, 10, 24, WHITE);
        DrawText("Sources (Green): Generate data - Single, Tick, Range, Array", 10, screenHeight - 100, 16, LIGHTGRAY);
        DrawText("Sinks (Pink): Consume data - Print, Sum, Count, Head", 10, screenHeight - 80, 16, LIGHTGRAY);
        DrawText("Flow (Gray): Transform data - Multiply, Convert to String", 10, screenHeight - 60, 16, LIGHTGRAY);
        DrawText("• Drag components • Connect ports • Press D for debug", 10, screenHeight - 40, 16, LIGHTGRAY);
        DrawText("Check console output for processing results", 10, screenHeight - 20, 16, LIGHTGRAY);
    }
    
    // Cleanup
    printf("\nShutting down demo...\n");
    
    // Stop components
    b8 stop = true;
    Component_push_control(source_single, &stop, sizeof(b8));
    Component_push_control(source_tick, &stop, sizeof(b8));
    Component_push_control(source_range, &stop, sizeof(b8));
    Component_push_control(source_array, &stop, sizeof(b8));
    Component_push_control(multiply_comp, &stop, sizeof(b8));
    Component_push_control(to_string_comp, &stop, sizeof(b8));
    Component_push_control(sink_print_num, &stop, sizeof(b8));
    Component_push_control(sink_sum, &stop, sizeof(b8));
    Component_push_control(sink_print_str, &stop, sizeof(b8));
    Component_push_control(sink_count, &stop, sizeof(b8));
    Component_push_control(sink_head, &stop, sizeof(b8));
    
    // Wait for components to finish
    Component_wait_end(source_single);
    Component_wait_end(source_tick);
    Component_wait_end(source_range);
    Component_wait_end(source_array);
    Component_wait_end(multiply_comp);
    Component_wait_end(to_string_comp);
    Component_wait_end(sink_print_num);
    Component_wait_end(sink_sum);
    Component_wait_end(sink_print_str);
    Component_wait_end(sink_count);
    Component_wait_end(sink_head);
    
    VisualEditor_destroy(editor);
    Arena_destroy(arena);
    
    CloseWindow();
    
    return 0;
}