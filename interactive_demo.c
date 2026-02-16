#define CSTREAM_IMPL
#define CSTREAM_SOURCES_IMPL
#include "visual_cstream.h"

int main() {
    // Initialize raylib
    const int screenWidth = 1400;
    const int screenHeight = 900;
    
    InitWindow(screenWidth, screenHeight, "CStream Interactive Visual Editor");
    SetTargetFPS(60);
    
    // Initialize cstream
    Arena *arena = Arena_create(1024 * 1024 * 1024);
    
    // Create visual editor with palette
    VisualEditor *editor = VisualEditor_create(arena);
    
    printf("=== CStream Interactive Visual Editor ===\n");
    printf("Features:\n");
    printf("• Drag components from palette (right panel) to create them\n");
    printf("• Connect output ports (red) to input ports (blue)\n");
    printf("• Move components by dragging\n");
    printf("• Press D for debug info, Delete to remove selected\n");
    printf("• Component types: Sources (green), Sinks (pink), Flow (gray)\n");
    printf("==========================================\n\n");
    
    // Main program loop
    int frame_count = 0;
    
    while (!WindowShouldClose() && frame_count < 18000) { // 5 minutes at 60 FPS
        frame_count++;
        
        // Handle input and update visual editor
        VisualEditor_handle_input(editor);
        VisualEditor_update(editor);
        
        // Render everything
        VisualEditor_render(editor);
        
        // Draw instructions
        DrawText("Interactive CStream Visual Editor", 10, 10, 24, WHITE);
        DrawText("Drag from Component Palette (right) to create components", 10, screenHeight - 120, 16, LIGHTGRAY);
        DrawText("• Sources (Green): Generate data automatically", 10, screenHeight - 100, 14, LIGHTGRAY);
        DrawText("• Sinks (Pink): Process and display results", 10, screenHeight - 80, 14, LIGHTGRAY);
        DrawText("• Flow (Gray): Transform data between sources and sinks", 10, screenHeight - 60, 14, LIGHTGRAY);
        DrawText("• Connect red output ports to blue input ports", 10, screenHeight - 40, 14, LIGHTGRAY);
        DrawText("• Check console for component output", 10, screenHeight - 20, 14, LIGHTGRAY);
        
        // Show component count
        char component_text[64];
        sprintf(component_text, "Components: %d", editor->num_components);
        DrawText(component_text, screenWidth - 200, 40, 16, WHITE);
        
        sprintf(component_text, "Connections: %d", editor->num_connections);
        DrawText(component_text, screenWidth - 200, 60, 16, WHITE);
    }
    
    // Cleanup - stop all created components
    printf("\nShutting down editor...\n");
    
    b8 stop = true;
    for (int i = 0; i < editor->num_components; i++) {
        Component* comp = editor->visual_components[i].component;
        if (comp) {
            Component_push_control(comp, &stop, sizeof(b8));
        }
    }
    
    // Wait for components to finish
    for (int i = 0; i < editor->num_components; i++) {
        Component* comp = editor->visual_components[i].component;
        if (comp) {
            Component_wait_end(comp);
        }
    }
    
    VisualEditor_destroy(editor);
    Arena_destroy(arena);
    
    CloseWindow();
    
    printf("Editor shutdown complete.\n");
    return 0;
}