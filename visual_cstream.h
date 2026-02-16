#pragma once
#include "cstream.h"
#include "cstream_sources.h"
#include "raylib.h"
#include <math.h>

#define MAX_VISUAL_COMPONENTS 32
#define MAX_CONNECTIONS 64
#define MAX_PALETTE_ITEMS 32
#define COMPONENT_WIDTH 120
#define COMPONENT_HEIGHT 80
#define PALETTE_WIDTH 200
#define PALETTE_ITEM_HEIGHT 40
#define PORT_RADIUS 8
#define PORT_SPACING 20

typedef enum ComponentVisualType {
    VISUAL_TYPE_FLOW,
    VISUAL_TYPE_SOURCE,  
    VISUAL_TYPE_SINK
} ComponentVisualType;

// Component palette item representing a component template
typedef struct ComponentPaletteItem {
    char name[64];
    char description[128];
    ComponentVisualType type;
    Color color;
    
    // Type-specific configuration
    union {
        struct {
            SourceType source_type;
            char config_hint[64];  // e.g., "Click to configure range"
        } source_config;
        
        struct {
            SinkType sink_type;
            char config_hint[64];
        } sink_config;
        
        struct {
            char transform_hint[64];
        } flow_config;
    } type_config;
    
    Rectangle bounds;  // UI bounds in palette
    bool is_hovered;
} ComponentPaletteItem;

// Component creation parameters (filled by user input)
typedef struct ComponentCreateParams {
    char name[64];
    
    union {
        struct {
            SourceType type;
            union {
                struct { i64 value; } single;
                struct { i64 value; u64 interval_ms; u64 max_ticks; } tick;
                struct { i64 start; i64 end; i64 step; } range;
                struct { i64 values[10]; u64 count; } array;
                struct { i64 value; u64 repeat_count; } repeat;
            } params;
        } source;
        
        struct {
            SinkType type;
        } sink;
        
        struct {
            char transform_name[64];
        } flow;
    } type_params;
} ComponentCreateParams;

typedef struct VisualPort {
    Vector2 position;
    bool is_input;
    int port_index;
    bool is_connected;
    Color color;
} VisualPort;

typedef struct VisualComponent {
    Component* component;
    ComponentVisualType visual_type;
    Vector2 position;
    Vector2 size;
    Color color;
    char* display_name;
    VisualPort input_ports[MAX_PORTS];
    VisualPort output_ports[MAX_PORTS];
    int num_input_ports;
    int num_output_ports;
    bool is_selected;
    bool is_dragging;
    Vector2 drag_offset;
    
    // Source/Sink specific visual info
    union {
        struct {
            SourceType source_type;
            char status_text[64];
        } source_info;
        
        struct {
            SinkType sink_type;  
            char status_text[64];
        } sink_info;
    } type_info;
} VisualComponent;

typedef struct Connection {
    int from_component_id;
    int from_port_index;
    int to_component_id;
    int to_port_index;
    Color color;
    bool is_active;
} Connection;

typedef struct VisualEditor {
    VisualComponent visual_components[MAX_VISUAL_COMPONENTS];
    Connection connections[MAX_CONNECTIONS];
    int num_components;
    int num_connections;
    
    // Component palette
    ComponentPaletteItem palette_items[MAX_PALETTE_ITEMS];
    int num_palette_items;
    bool show_palette;
    Rectangle palette_area;
    
    // Interaction state
    bool is_connecting;
    int connection_from_component;
    int connection_from_port;
    Vector2 temp_connection_end;
    
    // Palette drag state
    bool is_dragging_from_palette;
    int dragging_palette_item;
    Vector2 drag_start_pos;
    Vector2 drag_current_pos;
    
    // Component creation state
    bool show_create_dialog;
    ComponentCreateParams create_params;
    int creating_palette_item;
    
    // UI state
    Vector2 camera_offset;
    float zoom;
    bool show_debug;
    
    // Visual feedback state
    bool show_creation_feedback;
    char creation_feedback_text[128];
    float creation_feedback_timer;
    
    // Arena for creating components at runtime
    Arena* arena;
} VisualEditor;

// Visual editor functions
VisualEditor* VisualEditor_create(Arena* arena);
void VisualEditor_destroy(VisualEditor* editor);

// Component palette functions
void VisualEditor_init_palette(VisualEditor* editor);
void VisualEditor_render_palette(VisualEditor* editor);
void VisualEditor_handle_palette_input(VisualEditor* editor);
int VisualEditor_get_palette_item_at_position(VisualEditor* editor, Vector2 position);

// Component creation functions  
void VisualEditor_show_create_dialog(VisualEditor* editor, int palette_item);
void VisualEditor_render_create_dialog(VisualEditor* editor);
void VisualEditor_handle_create_dialog_input(VisualEditor* editor);
Component* VisualEditor_create_component_from_params(VisualEditor* editor, ComponentCreateParams* params);
int VisualEditor_instantiate_component(VisualEditor* editor, int palette_item, Vector2 position);

// Component management
int VisualEditor_add_component_internal(VisualEditor* editor, Component* component, Vector2 position, const char* name, ComponentVisualType visual_type, SourceType source_type, SinkType sink_type);
int VisualEditor_add_component(VisualEditor* editor, Component* component, Vector2 position, const char* name);
int VisualEditor_add_source_component(VisualEditor* editor, Component* component, Vector2 position, const char* name, SourceType source_type);
int VisualEditor_add_sink_component(VisualEditor* editor, Component* component, Vector2 position, const char* name, SinkType sink_type);
void VisualEditor_remove_component(VisualEditor* editor, int component_id);

// Connection management
bool VisualEditor_add_connection(VisualEditor* editor, int from_comp, int from_port, int to_comp, int to_port);
void VisualEditor_remove_connection(VisualEditor* editor, int connection_id);

// Interaction
void VisualEditor_handle_input(VisualEditor* editor);
void VisualEditor_update(VisualEditor* editor);
void VisualEditor_render(VisualEditor* editor);

// Helper functions
Vector2 VisualComponent_get_input_port_position(VisualComponent* comp, int port_index);
Vector2 VisualComponent_get_output_port_position(VisualComponent* comp, int port_index);
int VisualEditor_get_component_at_position(VisualEditor* editor, Vector2 position);
int VisualEditor_get_port_at_position(VisualEditor* editor, Vector2 position, bool* is_input, int* component_id);
void draw_arrow(Vector2 start, Vector2 end, Color color, float thickness);

// Feedback functions
void VisualEditor_show_feedback(VisualEditor* editor, const char* message);