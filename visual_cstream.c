#include "visual_cstream.h"
#include <string.h>

// Define additional colors
#define DARKRED (Color){139, 0, 0, 255}

// Data structure for numbers
typedef struct NumberData {
    i64 value;
} NumberData;

// Forward declarations for helper functions
void print_number_effect(void* element, void* context);
void* sum_fold(void* accumulator, void* element);
void* multiply_by_2(Component* comp, NumberData** data);
void* process_control(Component* comp, void* control_data);

// Helper function to get source type name
const char* get_source_type_name(SourceType type) {
    switch (type) {
        case SOURCE_SINGLE: return "Single";
        case SOURCE_TICK: return "Tick";
        case SOURCE_RANGE: return "Range";
        case SOURCE_FROM_ARRAY: return "Array";
        case SOURCE_REPEAT: return "Repeat";
        case SOURCE_UNFOLD: return "Unfold";
        case SOURCE_EMPTY: return "Empty";
        default: return "Unknown";
    }
}

// Helper function to get sink type name
const char* get_sink_type_name(SinkType type) {
    switch (type) {
        case SINK_FOREACH: return "ForEach";
        case SINK_FOLD: return "Fold";
        case SINK_HEAD: return "Head";
        case SINK_LAST: return "Last";
        case SINK_COLLECT: return "Collect";
        case SINK_COUNT: return "Count";
        case SINK_IGNORE: return "Ignore";
        default: return "Unknown";
    }
}

// Helper function to get component color based on type
Color get_component_color(ComponentVisualType type) {
    switch (type) {
        case VISUAL_TYPE_SOURCE: return (Color){144, 238, 144, 255}; // Light green
        case VISUAL_TYPE_SINK: return (Color){255, 182, 193, 255};   // Light pink
        case VISUAL_TYPE_FLOW: return LIGHTGRAY;
        default: return LIGHTGRAY;
    }
}

VisualEditor* VisualEditor_create(Arena* arena) {
    VisualEditor* editor = (VisualEditor*)malloc(sizeof(VisualEditor));
    memset(editor, 0, sizeof(VisualEditor));
    
    editor->zoom = 1.0f;
    editor->camera_offset = (Vector2){0, 0};
    editor->show_debug = false;
    editor->show_palette = true;
    editor->arena = arena;
    
    // Initialize palette area (right side of screen)
    editor->palette_area = (Rectangle){1200, 0, PALETTE_WIDTH, 900};
    
    // Initialize component palette
    VisualEditor_init_palette(editor);
    
    return editor;
}

void VisualEditor_destroy(VisualEditor* editor) {
    if (editor) {
        free(editor);
    }
}

void VisualEditor_init_palette(VisualEditor* editor) {
    int item_idx = 0;
    float y_offset = 50;
    
    // === SOURCES ===
    
    // Single Source
    ComponentPaletteItem* item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Single Source");
    strcpy(item->description, "Emits one value");
    item->type = VISUAL_TYPE_SOURCE;
    item->color = (Color){144, 238, 144, 255}; // Light green
    item->type_config.source_config.source_type = SOURCE_SINGLE;
    strcpy(item->type_config.source_config.config_hint, "Configure value");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Tick Source
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Tick Source");
    strcpy(item->description, "Emits at intervals");
    item->type = VISUAL_TYPE_SOURCE;
    item->color = (Color){144, 238, 144, 255};
    item->type_config.source_config.source_type = SOURCE_TICK;
    strcpy(item->type_config.source_config.config_hint, "Configure timing");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Range Source
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Range Source");
    strcpy(item->description, "Emits number range");
    item->type = VISUAL_TYPE_SOURCE;
    item->color = (Color){144, 238, 144, 255};
    item->type_config.source_config.source_type = SOURCE_RANGE;
    strcpy(item->type_config.source_config.config_hint, "Configure range");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Array Source
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Array Source");
    strcpy(item->description, "Emits from array");
    item->type = VISUAL_TYPE_SOURCE;
    item->color = (Color){144, 238, 144, 255};
    item->type_config.source_config.source_type = SOURCE_FROM_ARRAY;
    strcpy(item->type_config.source_config.config_hint, "Configure array");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 10;
    
    // === FLOW COMPONENTS ===
    
    // Math Transform
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Math Transform");
    strcpy(item->description, "Mathematical ops");
    item->type = VISUAL_TYPE_FLOW;
    item->color = LIGHTGRAY;
    strcpy(item->type_config.flow_config.transform_hint, "Multiply, add, etc.");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Type Transform
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Type Transform");
    strcpy(item->description, "Convert data types");
    item->type = VISUAL_TYPE_FLOW;
    item->color = LIGHTGRAY;
    strcpy(item->type_config.flow_config.transform_hint, "Number to string");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 10;
    
    // === SINKS ===
    
    // Print Sink
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Print Sink");
    strcpy(item->description, "Prints to console");
    item->type = VISUAL_TYPE_SINK;
    item->color = (Color){255, 182, 193, 255}; // Light pink
    item->type_config.sink_config.sink_type = SINK_FOREACH;
    strcpy(item->type_config.sink_config.config_hint, "Prints each element");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Sum Sink
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Sum Sink");
    strcpy(item->description, "Sums all values");
    item->type = VISUAL_TYPE_SINK;
    item->color = (Color){255, 182, 193, 255};
    item->type_config.sink_config.sink_type = SINK_FOLD;
    strcpy(item->type_config.sink_config.config_hint, "Accumulates sum");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Count Sink
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Count Sink");
    strcpy(item->description, "Counts elements");
    item->type = VISUAL_TYPE_SINK;
    item->color = (Color){255, 182, 193, 255};
    item->type_config.sink_config.sink_type = SINK_COUNT;
    strcpy(item->type_config.sink_config.config_hint, "Counts total");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    // Head Sink
    item = &editor->palette_items[item_idx++];
    strcpy(item->name, "Head Sink");
    strcpy(item->description, "Takes first element");
    item->type = VISUAL_TYPE_SINK;
    item->color = (Color){255, 182, 193, 255};
    item->type_config.sink_config.sink_type = SINK_HEAD;
    strcpy(item->type_config.sink_config.config_hint, "First element only");
    item->bounds = (Rectangle){editor->palette_area.x + 10, y_offset, PALETTE_WIDTH - 20, PALETTE_ITEM_HEIGHT};
    y_offset += PALETTE_ITEM_HEIGHT + 5;
    
    editor->num_palette_items = item_idx;
}

void VisualEditor_render_palette(VisualEditor* editor) {
    if (!editor->show_palette) return;
    
    // Draw palette background
    DrawRectangleRec(editor->palette_area, (Color){50, 50, 50, 200});
    DrawRectangleLinesEx(editor->palette_area, 2, WHITE);
    
    // Draw palette title
    DrawText("Component Palette", editor->palette_area.x + 10, 10, 16, WHITE);
    DrawText("Drag to create components", editor->palette_area.x + 10, 30, 12, LIGHTGRAY);
    
    // Draw palette items
    for (int i = 0; i < editor->num_palette_items; i++) {
        ComponentPaletteItem* item = &editor->palette_items[i];
        
        // Highlight if hovered or dragging
        Color item_color;
        if (editor->is_dragging_from_palette && editor->dragging_palette_item == i) {
            // Being dragged - semi-transparent
            item_color = (Color){item->color.r, item->color.g, item->color.b, 150};
        } else if (item->is_hovered) {
            // Hovered - brighter
            item_color = (Color){
                (item->color.r + 50 > 255) ? 255 : item->color.r + 50,
                (item->color.g + 50 > 255) ? 255 : item->color.g + 50,
                (item->color.b + 50 > 255) ? 255 : item->color.b + 50,
                255
            };
        } else {
            // Normal
            item_color = item->color;
        }
            
        // Draw item background
        DrawRectangleRec(item->bounds, item_color);
        DrawRectangleLinesEx(item->bounds, 1, BLACK);
        
        // Draw type icon
        Vector2 icon_pos = {item->bounds.x + 5, item->bounds.y + 5};
        if (item->type == VISUAL_TYPE_SOURCE) {
            // Triangle for source
            Vector2 p1 = {icon_pos.x, icon_pos.y};
            Vector2 p2 = {icon_pos.x, icon_pos.y + 12};
            Vector2 p3 = {icon_pos.x + 10, icon_pos.y + 6};
            DrawTriangle(p1, p2, p3, DARKGREEN);
        } else if (item->type == VISUAL_TYPE_SINK) {
            // Square for sink
            Rectangle icon_rect = {icon_pos.x, icon_pos.y, 12, 12};
            DrawRectangleRec(icon_rect, DARKRED);
            DrawRectangleLinesEx(icon_rect, 1, BLACK);
        } else {
            // Diamond for flow
            Vector2 d_center = {icon_pos.x + 6, icon_pos.y + 6};
            Vector2 d1 = {d_center.x, d_center.y - 6};
            Vector2 d2 = {d_center.x + 6, d_center.y};
            Vector2 d3 = {d_center.x, d_center.y + 6};
            Vector2 d4 = {d_center.x - 6, d_center.y};
            DrawTriangle(d1, d2, d3, DARKBLUE);
            DrawTriangle(d1, d3, d4, DARKBLUE);
        }
        
        // Draw item text
        DrawText(item->name, item->bounds.x + 20, item->bounds.y + 5, 12, BLACK);
        DrawText(item->description, item->bounds.x + 20, item->bounds.y + 20, 10, (Color){60, 60, 60, 255});
    }
    
    // Draw dragging preview
    if (editor->is_dragging_from_palette && editor->dragging_palette_item >= 0) {
        ComponentPaletteItem* item = &editor->palette_items[editor->dragging_palette_item];
        
        // Draw semi-transparent preview at mouse position
        Rectangle preview_rect = {
            editor->drag_current_pos.x - COMPONENT_WIDTH/2,
            editor->drag_current_pos.y - COMPONENT_HEIGHT/2,
            COMPONENT_WIDTH,
            COMPONENT_HEIGHT
        };
        
        Color preview_color = item->color;
        preview_color.a = 150; // Semi-transparent
        
        DrawRectangleRec(preview_rect, preview_color);
        DrawRectangleLinesEx(preview_rect, 2, WHITE);
        DrawText(item->name, preview_rect.x + 5, preview_rect.y + 5, 12, BLACK);
    }
}

int VisualEditor_add_component(VisualEditor* editor, Component* component, Vector2 position, const char* name) {
    return VisualEditor_add_component_internal(editor, component, position, name, VISUAL_TYPE_FLOW, 0, 0);
}

int VisualEditor_add_source_component(VisualEditor* editor, Component* component, Vector2 position, const char* name, SourceType source_type) {
    return VisualEditor_add_component_internal(editor, component, position, name, VISUAL_TYPE_SOURCE, source_type, 0);
}

int VisualEditor_add_sink_component(VisualEditor* editor, Component* component, Vector2 position, const char* name, SinkType sink_type) {
    return VisualEditor_add_component_internal(editor, component, position, name, VISUAL_TYPE_SINK, 0, sink_type);
}

// Internal function to add components with type information
int VisualEditor_add_component_internal(VisualEditor* editor, Component* component, Vector2 position, const char* name, ComponentVisualType visual_type, SourceType source_type, SinkType sink_type) {
    if (editor->num_components >= MAX_VISUAL_COMPONENTS) {
        return -1;
    }
    
    // Null check for component
    if (component == NULL) {
        printf("ERROR: Attempted to add NULL component\n");
        return -1;
    }
    
    int id = editor->num_components++;
    VisualComponent* visual_comp = &editor->visual_components[id];
    
    visual_comp->component = component;
    visual_comp->visual_type = visual_type;
    visual_comp->position = position;
    visual_comp->size = (Vector2){COMPONENT_WIDTH, COMPONENT_HEIGHT};
    visual_comp->color = get_component_color(visual_type);
    visual_comp->display_name = (char*)malloc(strlen(name) + 1);
    strcpy(visual_comp->display_name, name);
    visual_comp->is_selected = false;
    visual_comp->is_dragging = false;
    
    // Set type-specific information
    if (visual_type == VISUAL_TYPE_SOURCE) {
        visual_comp->type_info.source_info.source_type = source_type;
        sprintf(visual_comp->type_info.source_info.status_text, "%s Source", get_source_type_name(source_type));
    } else if (visual_type == VISUAL_TYPE_SINK) {
        visual_comp->type_info.sink_info.sink_type = sink_type;
        sprintf(visual_comp->type_info.sink_info.status_text, "%s Sink", get_sink_type_name(sink_type));
    }
    
    // Set up input ports - sources typically have no inputs, sinks have inputs
    // For simplicity, sources/sinks/flow components in our system use port 0 only
    visual_comp->num_input_ports = 0;
    if (visual_type != VISUAL_TYPE_SOURCE) {
        // Sinks and Flow components have at least one input port (port 0)
        visual_comp->input_ports[0] = (VisualPort){
            .position = VisualComponent_get_input_port_position(visual_comp, 0),
            .is_input = true,
            .port_index = 0,
            .is_connected = false,
            .color = BLUE
        };
        visual_comp->num_input_ports = 1;
    }
    
    // Set up output ports - sinks typically have no outputs, sources have outputs  
    visual_comp->num_output_ports = 0;
    if (visual_type != VISUAL_TYPE_SINK) {
        // Sources and Flow components have at least one output port (port 0)
        visual_comp->output_ports[0] = (VisualPort){
            .position = VisualComponent_get_output_port_position(visual_comp, 0),
            .is_input = false,
            .port_index = 0,
            .is_connected = false,
            .color = RED
        };
        visual_comp->num_output_ports = 1;
    }
    
    return id;
}

void VisualEditor_remove_component(VisualEditor* editor, int component_id) {
    if (component_id < 0 || component_id >= editor->num_components) return;
    
    VisualComponent* comp = &editor->visual_components[component_id];
    if (comp->display_name) {
        free(comp->display_name);
    }
    
    // Remove connections involving this component
    for (int i = editor->num_connections - 1; i >= 0; i--) {
        Connection* conn = &editor->connections[i];
        if (conn->from_component_id == component_id || conn->to_component_id == component_id) {
            VisualEditor_remove_connection(editor, i);
        }
    }
    
    // Shift remaining components
    for (int i = component_id; i < editor->num_components - 1; i++) {
        editor->visual_components[i] = editor->visual_components[i + 1];
    }
    editor->num_components--;
}

bool VisualEditor_add_connection(VisualEditor* editor, int from_comp, int from_port, int to_comp, int to_port) {
    if (editor->num_connections >= MAX_CONNECTIONS) return false;
    if (from_comp < 0 || from_comp >= editor->num_components) return false;
    if (to_comp < 0 || to_comp >= editor->num_components) return false;
    
    // Connect the actual cstream components
    VisualComponent* from_visual = &editor->visual_components[from_comp];
    VisualComponent* to_visual = &editor->visual_components[to_comp];
    
    // Link the data streams
    to_visual->component->data_in[to_port] = from_visual->component->data_out[from_port];
    
    // Add visual connection
    int id = editor->num_connections++;
    Connection* conn = &editor->connections[id];
    conn->from_component_id = from_comp;
    conn->from_port_index = from_port;
    conn->to_component_id = to_comp;
    conn->to_port_index = to_port;
    conn->color = GREEN;
    conn->is_active = true;
    
    // Mark ports as connected
    from_visual->output_ports[from_port].is_connected = true;
    to_visual->input_ports[to_port].is_connected = true;
    
    return true;
}

void VisualEditor_remove_connection(VisualEditor* editor, int connection_id) {
    if (connection_id < 0 || connection_id >= editor->num_connections) return;
    
    Connection* conn = &editor->connections[connection_id];
    
    // Disconnect cstream components
    VisualComponent* to_visual = &editor->visual_components[conn->to_component_id];
    to_visual->component->data_in[conn->to_port_index] = NULL;
    
    // Mark ports as not connected
    VisualComponent* from_visual = &editor->visual_components[conn->from_component_id];
    from_visual->output_ports[conn->from_port_index].is_connected = false;
    to_visual->input_ports[conn->to_port_index].is_connected = false;
    
    // Shift remaining connections
    for (int i = connection_id; i < editor->num_connections - 1; i++) {
        editor->connections[i] = editor->connections[i + 1];
    }
    editor->num_connections--;
}

void VisualEditor_handle_input(VisualEditor* editor) {
    // Handle palette input first
    VisualEditor_handle_palette_input(editor);
    
    // Skip normal input handling if dragging from palette
    if (editor->is_dragging_from_palette) {
        return;
    }
    
    Vector2 mouse_pos = GetMousePosition();
    Vector2 world_mouse_pos = {
        (mouse_pos.x - editor->camera_offset.x) / editor->zoom,
        (mouse_pos.y - editor->camera_offset.y) / editor->zoom
    };
    
    // Handle mouse input
    if (IsMouseButtonPressed(MOUSE_BUTTON_LEFT)) {
        // Check if clicking on a port to start connection
        bool is_input;
        int component_id;
        int port_id = VisualEditor_get_port_at_position(editor, world_mouse_pos, &is_input, &component_id);
        
        if (port_id >= 0 && !is_input) {
            // Start connection from output port
            editor->is_connecting = true;
            editor->connection_from_component = component_id;
            editor->connection_from_port = port_id;
            editor->temp_connection_end = world_mouse_pos;
        } else {
            // Check if clicking on a component
            int comp_id = VisualEditor_get_component_at_position(editor, world_mouse_pos);
            if (comp_id >= 0) {
                VisualComponent* comp = &editor->visual_components[comp_id];
                comp->is_selected = true;
                comp->is_dragging = true;
                comp->drag_offset = (Vector2){
                    world_mouse_pos.x - comp->position.x,
                    world_mouse_pos.y - comp->position.y
                };
                
                // Deselect other components
                for (int i = 0; i < editor->num_components; i++) {
                    if (i != comp_id) {
                        editor->visual_components[i].is_selected = false;
                    }
                }
            } else {
                // Deselect all
                for (int i = 0; i < editor->num_components; i++) {
                    editor->visual_components[i].is_selected = false;
                }
            }
        }
    }
    
    if (IsMouseButtonReleased(MOUSE_BUTTON_LEFT)) {
        if (editor->is_connecting) {
            // Try to complete connection
            bool is_input;
            int component_id;
            int port_id = VisualEditor_get_port_at_position(editor, world_mouse_pos, &is_input, &component_id);
            
            if (port_id >= 0 && is_input && component_id != editor->connection_from_component) {
                bool success = VisualEditor_add_connection(editor, 
                    editor->connection_from_component, editor->connection_from_port,
                    component_id, port_id);
                
                if (success) {
                    VisualEditor_show_feedback(editor, "Components connected successfully!");
                } else {
                    VisualEditor_show_feedback(editor, "Connection failed!");
                }
            }
            
            editor->is_connecting = false;
        }
        
        // Stop dragging all components
        for (int i = 0; i < editor->num_components; i++) {
            editor->visual_components[i].is_dragging = false;
        }
    }
    
    // Handle dragging
    if (IsMouseButtonDown(MOUSE_BUTTON_LEFT)) {
        if (editor->is_connecting) {
            editor->temp_connection_end = world_mouse_pos;
        }
        
        for (int i = 0; i < editor->num_components; i++) {
            VisualComponent* comp = &editor->visual_components[i];
            if (comp->is_dragging) {
                comp->position = (Vector2){
                    world_mouse_pos.x - comp->drag_offset.x,
                    world_mouse_pos.y - comp->drag_offset.y
                };
                
                // Update port positions
                for (int j = 0; j < comp->num_input_ports; j++) {
                    comp->input_ports[j].position = VisualComponent_get_input_port_position(comp, j);
                }
                for (int j = 0; j < comp->num_output_ports; j++) {
                    comp->output_ports[j].position = VisualComponent_get_output_port_position(comp, j);
                }
            }
        }
    }
    
    // Handle keyboard shortcuts
    if (IsKeyPressed(KEY_D)) {
        editor->show_debug = !editor->show_debug;
    }
    
    if (IsKeyPressed(KEY_ESCAPE)) {
        // Deselect all components
        for (int i = 0; i < editor->num_components; i++) {
            editor->visual_components[i].is_selected = false;
        }
        VisualEditor_show_feedback(editor, "All components deselected");
    }
    
    if (IsKeyPressed(KEY_DELETE)) {
        // Delete selected components
        for (int i = editor->num_components - 1; i >= 0; i--) {
            if (editor->visual_components[i].is_selected) {
                VisualEditor_remove_component(editor, i);
            }
        }
    }
}

void VisualEditor_update(VisualEditor* editor) {
    // Update any animations or time-based effects here
}

void VisualEditor_render(VisualEditor* editor) {
    BeginDrawing();
    ClearBackground(DARKGRAY);
    
    // Apply camera transform
    // Note: For simplicity, we'll handle this manually in each draw call
    
    // Draw grid
    for (int x = -1000; x < 2000; x += 50) {
        Vector2 start = {x + editor->camera_offset.x, -1000 + editor->camera_offset.y};
        Vector2 end = {x + editor->camera_offset.x, 2000 + editor->camera_offset.y};
        DrawLineV(start, end, (Color){100, 100, 100, 50});
    }
    for (int y = -1000; y < 2000; y += 50) {
        Vector2 start = {-1000 + editor->camera_offset.x, y + editor->camera_offset.y};
        Vector2 end = {2000 + editor->camera_offset.x, y + editor->camera_offset.y};
        DrawLineV(start, end, (Color){100, 100, 100, 50});
    }
    
    // Draw connections
    for (int i = 0; i < editor->num_connections; i++) {
        Connection* conn = &editor->connections[i];
        if (!conn->is_active) continue;
        
        VisualComponent* from_comp = &editor->visual_components[conn->from_component_id];
        VisualComponent* to_comp = &editor->visual_components[conn->to_component_id];
        
        Vector2 start = VisualComponent_get_output_port_position(from_comp, conn->from_port_index);
        Vector2 end = VisualComponent_get_input_port_position(to_comp, conn->to_port_index);
        
        // Apply camera transform
        start.x = start.x * editor->zoom + editor->camera_offset.x;
        start.y = start.y * editor->zoom + editor->camera_offset.y;
        end.x = end.x * editor->zoom + editor->camera_offset.x;
        end.y = end.y * editor->zoom + editor->camera_offset.y;
        
        draw_arrow(start, end, conn->color, 3.0f);
    }
    
    // Draw temporary connection while dragging
    if (editor->is_connecting) {
        VisualComponent* from_comp = &editor->visual_components[editor->connection_from_component];
        Vector2 start = VisualComponent_get_output_port_position(from_comp, editor->connection_from_port);
        Vector2 end = editor->temp_connection_end;
        
        // Apply camera transform
        start.x = start.x * editor->zoom + editor->camera_offset.x;
        start.y = start.y * editor->zoom + editor->camera_offset.y;
        end.x = end.x * editor->zoom + editor->camera_offset.x;
        end.y = end.y * editor->zoom + editor->camera_offset.y;
        
        draw_arrow(start, end, YELLOW, 2.0f);
    }
    
    // Draw components
    for (int i = 0; i < editor->num_components; i++) {
        VisualComponent* comp = &editor->visual_components[i];
        
        // Apply camera transform
        Rectangle rect = {
            comp->position.x * editor->zoom + editor->camera_offset.x,
            comp->position.y * editor->zoom + editor->camera_offset.y,
            comp->size.x * editor->zoom,
            comp->size.y * editor->zoom
        };
        
        // Draw component box
        Color box_color = comp->is_selected ? GOLD : comp->color;
        DrawRectangleRec(rect, box_color);
        DrawRectangleLinesEx(rect, 2, BLACK);
        
        // Draw component name
        Vector2 text_pos = {
            rect.x + 5,
            rect.y + 5
        };
        DrawText(comp->display_name, (int)text_pos.x, (int)text_pos.y, 12, BLACK);
        
        // Draw type-specific information
        if (comp->visual_type == VISUAL_TYPE_SOURCE) {
            Vector2 type_pos = {
                rect.x + 5,
                rect.y + 20
            };
            DrawText(comp->type_info.source_info.status_text, (int)type_pos.x, (int)type_pos.y, 10, DARKGREEN);
            
            // Draw source icon (triangle pointing right)
            Vector2 icon_center = {rect.x + rect.width - 20, rect.y + 15};
            Vector2 p1 = {icon_center.x - 8, icon_center.y - 6};
            Vector2 p2 = {icon_center.x - 8, icon_center.y + 6};
            Vector2 p3 = {icon_center.x + 4, icon_center.y};
            DrawTriangle(p1, p2, p3, DARKGREEN);
            
        } else if (comp->visual_type == VISUAL_TYPE_SINK) {
            Vector2 type_pos = {
                rect.x + 5,
                rect.y + 20
            };
            DrawText(comp->type_info.sink_info.status_text, (int)type_pos.x, (int)type_pos.y, 10, DARKRED);
            
            // Draw sink icon (square)
            Rectangle sink_icon = {rect.x + rect.width - 20, rect.y + 10, 12, 12};
            DrawRectangleRec(sink_icon, DARKRED);
            DrawRectangleLinesEx(sink_icon, 1, BLACK);
        } else {
            // Flow component - draw flow icon (diamond)
            Vector2 diamond_center = {rect.x + rect.width - 15, rect.y + 15};
            Vector2 d1 = {diamond_center.x, diamond_center.y - 8};
            Vector2 d2 = {diamond_center.x + 8, diamond_center.y};
            Vector2 d3 = {diamond_center.x, diamond_center.y + 8};
            Vector2 d4 = {diamond_center.x - 8, diamond_center.y};
            DrawTriangle(d1, d2, d3, DARKBLUE);
            DrawTriangle(d1, d3, d4, DARKBLUE);
        }
        
        // Draw input ports
        for (int j = 0; j < comp->num_input_ports; j++) {
            VisualPort* port = &comp->input_ports[j];
            Vector2 port_pos = {
                port->position.x * editor->zoom + editor->camera_offset.x,
                port->position.y * editor->zoom + editor->camera_offset.y
            };
            Color port_color = port->is_connected ? BLUE : port->color;
            DrawCircleV(port_pos, PORT_RADIUS * editor->zoom, port_color);
            DrawCircleLinesV(port_pos, PORT_RADIUS * editor->zoom, BLACK);
        }
        
        // Draw output ports
        for (int j = 0; j < comp->num_output_ports; j++) {
            VisualPort* port = &comp->output_ports[j];
            Vector2 port_pos = {
                port->position.x * editor->zoom + editor->camera_offset.x,
                port->position.y * editor->zoom + editor->camera_offset.y
            };
            Color port_color = port->is_connected ? RED : port->color;
            DrawCircleV(port_pos, PORT_RADIUS * editor->zoom, port_color);
            DrawCircleLinesV(port_pos, PORT_RADIUS * editor->zoom, BLACK);
        }
    }
    
    // Draw debug info
    if (editor->show_debug) {
        char debug_text[256];
        sprintf(debug_text, "Components: %d", editor->num_components);
        DrawText(debug_text, 10, 10, 16, WHITE);
        
        sprintf(debug_text, "Connections: %d", editor->num_connections);
        DrawText(debug_text, 10, 30, 16, WHITE);
        
        sprintf(debug_text, "Camera: %.1f, %.1f", editor->camera_offset.x, editor->camera_offset.y);
        DrawText(debug_text, 10, 50, 16, WHITE);
        
        sprintf(debug_text, "Zoom: %.2f", editor->zoom);
        DrawText(debug_text, 10, 70, 16, WHITE);
        
        DrawText("=== Keyboard Shortcuts ===", 10, 100, 16, YELLOW);
        DrawText("D - Toggle debug info", 10, 120, 14, WHITE);
        DrawText("Delete - Remove selected components", 10, 140, 14, WHITE);
        DrawText("ESC - Deselect all", 10, 160, 14, WHITE);
        
        DrawText("=== Mouse Controls ===", 10, 190, 16, YELLOW);
        DrawText("Left Click - Select component", 10, 210, 14, WHITE);
        DrawText("Drag - Move components", 10, 230, 14, WHITE);
        DrawText("Drag from palette - Create component", 10, 250, 14, WHITE);
        DrawText("Drag red -> blue - Connect components", 10, 270, 14, WHITE);
    }
    
    // Render component palette
    VisualEditor_render_palette(editor);
    
    // Render creation feedback
    if (editor->show_creation_feedback && editor->creation_feedback_timer > 0) {
        // Draw feedback message in the center top
        Vector2 text_size = MeasureTextEx(GetFontDefault(), editor->creation_feedback_text, 18, 1);
        Vector2 text_pos = {
            (GetScreenWidth() - text_size.x) / 2,
            50
        };
        
        // Background
        Rectangle bg_rect = {
            text_pos.x - 10, text_pos.y - 5,
            text_size.x + 20, text_size.y + 10
        };
        DrawRectangleRounded(bg_rect, 0.3f, 8, (Color){0, 150, 0, 200});
        DrawRectangleRoundedLines(bg_rect, 0.3f, 8, WHITE);
        
        // Text
        DrawText(editor->creation_feedback_text, (int)text_pos.x, (int)text_pos.y, 18, WHITE);
        
        // Update timer
        editor->creation_feedback_timer -= GetFrameTime();
        if (editor->creation_feedback_timer <= 0) {
            editor->show_creation_feedback = false;
        }
    }
    
    EndDrawing();
}

Vector2 VisualComponent_get_input_port_position(VisualComponent* comp, int port_index) {
    float port_y = comp->position.y + PORT_SPACING + (port_index * (PORT_RADIUS * 2 + 10));
    return (Vector2){comp->position.x - PORT_RADIUS, port_y};
}

Vector2 VisualComponent_get_output_port_position(VisualComponent* comp, int port_index) {
    float port_y = comp->position.y + PORT_SPACING + (port_index * (PORT_RADIUS * 2 + 10));
    return (Vector2){comp->position.x + comp->size.x + PORT_RADIUS, port_y};
}

int VisualEditor_get_component_at_position(VisualEditor* editor, Vector2 position) {
    for (int i = 0; i < editor->num_components; i++) {
        VisualComponent* comp = &editor->visual_components[i];
        Rectangle rect = {comp->position.x, comp->position.y, comp->size.x, comp->size.y};
        if (CheckCollisionPointRec(position, rect)) {
            return i;
        }
    }
    return -1;
}

int VisualEditor_get_port_at_position(VisualEditor* editor, Vector2 position, bool* is_input, int* component_id) {
    for (int i = 0; i < editor->num_components; i++) {
        VisualComponent* comp = &editor->visual_components[i];
        
        // Check input ports
        for (int j = 0; j < comp->num_input_ports; j++) {
            Vector2 port_pos = comp->input_ports[j].position;
            if (CheckCollisionPointCircle(position, port_pos, PORT_RADIUS)) {
                *is_input = true;
                *component_id = i;
                return j;
            }
        }
        
        // Check output ports
        for (int j = 0; j < comp->num_output_ports; j++) {
            Vector2 port_pos = comp->output_ports[j].position;
            if (CheckCollisionPointCircle(position, port_pos, PORT_RADIUS)) {
                *is_input = false;
                *component_id = i;
                return j;
            }
        }
    }
    return -1;
}

void draw_arrow(Vector2 start, Vector2 end, Color color, float thickness) {
    DrawLineEx(start, end, thickness, color);
    
    // Calculate arrow head
    Vector2 direction = {end.x - start.x, end.y - start.y};
    float length = sqrtf(direction.x * direction.x + direction.y * direction.y);
    if (length > 0) {
        direction.x /= length;
        direction.y /= length;
        
        Vector2 arrow_head1 = {
            end.x - direction.x * 10 + direction.y * 5,
            end.y - direction.y * 10 - direction.x * 5
        };
        Vector2 arrow_head2 = {
            end.x - direction.x * 10 - direction.y * 5,
            end.y - direction.y * 10 + direction.x * 5
        };
        
        DrawTriangle(end, arrow_head1, arrow_head2, color);
    }
}

int VisualEditor_get_palette_item_at_position(VisualEditor* editor, Vector2 position) {
    for (int i = 0; i < editor->num_palette_items; i++) {
        if (CheckCollisionPointRec(position, editor->palette_items[i].bounds)) {
            return i;
        }
    }
    return -1;
}

void VisualEditor_handle_palette_input(VisualEditor* editor) {
    Vector2 mouse_pos = GetMousePosition();
    
    // Update hover states
    for (int i = 0; i < editor->num_palette_items; i++) {
        editor->palette_items[i].is_hovered = 
            CheckCollisionPointRec(mouse_pos, editor->palette_items[i].bounds);
    }
    
    // Handle drag from palette
    if (IsMouseButtonPressed(MOUSE_BUTTON_LEFT)) {
        int palette_item = VisualEditor_get_palette_item_at_position(editor, mouse_pos);
        if (palette_item >= 0) {
            editor->is_dragging_from_palette = true;
            editor->dragging_palette_item = palette_item;
            editor->drag_start_pos = mouse_pos;
            editor->drag_current_pos = mouse_pos;
        }
    }
    
    if (editor->is_dragging_from_palette) {
        editor->drag_current_pos = mouse_pos;
        
        if (IsMouseButtonReleased(MOUSE_BUTTON_LEFT)) {
            // Check if dropped in main area (not on palette)
            if (!CheckCollisionPointRec(mouse_pos, editor->palette_area)) {
                // Convert to world coordinates
                Vector2 world_pos = {
                    (mouse_pos.x - editor->camera_offset.x) / editor->zoom,
                    (mouse_pos.y - editor->camera_offset.y) / editor->zoom
                };
                
                // Create component at drop position
                VisualEditor_instantiate_component(editor, editor->dragging_palette_item, world_pos);
            }
            
            editor->is_dragging_from_palette = false;
            editor->dragging_palette_item = -1;
        }
    }
}

// Simple component instantiation (with default parameters)
int VisualEditor_instantiate_component(VisualEditor* editor, int palette_item, Vector2 position) {
    if (palette_item < 0 || palette_item >= editor->num_palette_items) {
        return -1;
    }
    
    ComponentPaletteItem* item = &editor->palette_items[palette_item];
    Component* component = NULL;
    int visual_id = -1;
    
    // Create component with default parameters based on type
    switch (item->type) {
        case VISUAL_TYPE_SOURCE: {
            switch (item->type_config.source_config.source_type) {
                case SOURCE_SINGLE: {
                    static int single_counter = 1;
                    NumberData value = {single_counter * 10};
                    component = Source_single(editor->arena, &value, sizeof(NumberData), "Single");
                    
                    char name[64];
                    sprintf(name, "Single(%d)", (int)value.value);
                    visual_id = VisualEditor_add_source_component(editor, component, position, name, SOURCE_SINGLE);
                    single_counter++;
                    break;
                }
                case SOURCE_TICK: {
                    NumberData value = {1};
                    component = Source_tick(editor->arena, &value, sizeof(NumberData), 1000, 10, "Tick");
                    visual_id = VisualEditor_add_source_component(editor, component, position, "Tick(1s)", SOURCE_TICK);
                    break;
                }
                case SOURCE_RANGE: {
                    static int range_counter = 1;
                    component = Source_range(editor->arena, range_counter, range_counter + 5, 1, "Range");
                    
                    char name[64];
                    sprintf(name, "Range(%d..%d)", range_counter, range_counter + 4);
                    visual_id = VisualEditor_add_source_component(editor, component, position, name, SOURCE_RANGE);
                    range_counter += 5;
                    break;
                }
                case SOURCE_FROM_ARRAY: {
                    static i64 numbers[] = {10, 20, 30, 40, 50};
                    component = Source_from_array(editor->arena, numbers, sizeof(i64), 5, "Array");
                    visual_id = VisualEditor_add_source_component(editor, component, position, "Array[5]", SOURCE_FROM_ARRAY);
                    break;
                }
                default:
                    break;
            }
            
            // Note: Source components auto-start their threads upon creation
            // Do NOT call Component_start() here - it would start extra threads with wrong function
            if (component) {
                VisualEditor_show_feedback(editor, "Source component created and started!");
            }
            break;
        }
        
        case VISUAL_TYPE_SINK: {
            switch (item->type_config.sink_config.sink_type) {
                case SINK_FOREACH: {
                    component = Sink_foreach(editor->arena, print_number_effect, NULL, sizeof(NumberData), "Print");
                    visual_id = VisualEditor_add_sink_component(editor, component, position, "Print", SINK_FOREACH);
                    break;
                }
                case SINK_FOLD: {
                    NumberData initial = {0};
                    component = Sink_fold(editor->arena, sum_fold, &initial, sizeof(NumberData), sizeof(NumberData), "Sum");
                    visual_id = VisualEditor_add_sink_component(editor, component, position, "Sum", SINK_FOLD);
                    break;
                }
                case SINK_COUNT: {
                    component = Sink_count(editor->arena, sizeof(NumberData), "Count");
                    visual_id = VisualEditor_add_sink_component(editor, component, position, "Count", SINK_COUNT);
                    break;
                }
                case SINK_HEAD: {
                    component = Sink_head(editor->arena, sizeof(NumberData), "Head");
                    visual_id = VisualEditor_add_sink_component(editor, component, position, "Head", SINK_HEAD);
                    break;
                }
                default:
                    break;
            }
            
            // Note: Sink components auto-start their threads upon creation
            // Do NOT call Component_start() here - it would start extra threads with wrong function
            if (component) {
                VisualEditor_show_feedback(editor, "Sink component created and started!");
            }
            break;
        }
        
        case VISUAL_TYPE_FLOW: {
            // For now, create a simple multiply transform
            component = COMP_FLOW(NumberData*, NumberData*, b8, "Transform", editor->arena,
                (void*)multiply_by_2, (void*)process_control, 1, NULL);
            visual_id = VisualEditor_add_component(editor, component, position, "Multiply x2");
            
            if (component) {
                Component_start(component);
                VisualEditor_show_feedback(editor, "Flow component created and started!");
            }
            break;
        }
    }
    
    return visual_id;
}

// Helper functions that need to be defined somewhere accessible
void print_number_effect(void* element, void* context) {
    NumberData* num = (NumberData*)element;
    printf("Sink received: %lld\n", num->value);
}

void* sum_fold(void* accumulator, void* element) {
    NumberData* acc = (NumberData*)accumulator;
    NumberData* num = (NumberData*)element;
    acc->value += num->value;
    return acc;
}

void* multiply_by_2(Component* comp, NumberData** data) {
    NumberData* input = *data;
    static NumberData output;
    output.value = input->value * 2;
    return &output;
}

void* process_control(Component* comp, void* control_data) {
    b8 *stop = (b8*)(control_data);
    if (*stop) { pthread_exit(NULL); }
    return NULL;
}

void VisualEditor_show_feedback(VisualEditor* editor, const char* message) {
    strncpy(editor->creation_feedback_text, message, sizeof(editor->creation_feedback_text) - 1);
    editor->creation_feedback_text[sizeof(editor->creation_feedback_text) - 1] = '\0';
    editor->show_creation_feedback = true;
    editor->creation_feedback_timer = 3.0f; // Show for 3 seconds
}