# CStream Visual Programming Interface

This project adds a visual block and arrow programming interface to the CStream component-based streaming system using raylib, with support for Akka Streams-inspired source and sink components.

## What's Implemented

### Core Files

- **`visual_cstream.h`** - Header file defining the visual editor data structures and API
- **`visual_cstream.c`** - Implementation of the visual programming interface  
- **`visual_main.c`** - Main application demonstrating basic visual interface
- **`cstream_sources.h`** - Source and sink component definitions inspired by Akka Streams
- **`sources_demo.c`** - Comprehensive demo showcasing various source and sink types
- **`cstream.h`** - Modified to use stb-style single header with `CSTREAM_IMPL` define

### Visual Programming Features

1. **Visual Components**: Components are rendered as rectangular blocks with:
   - Component name displayed on the block
   - Blue input ports on the left side (for Flow and Sink components)
   - Red output ports on the right side (for Source and Flow components)
   - Type-specific colors:
     - **Green**: Source components (data generators)
     - **Pink**: Sink components (data consumers)  
     - **Gray**: Flow components (data transformers)
   - Visual icons indicating component type:
     - **Triangle**: Source components
     - **Square**: Sink components
     - **Diamond**: Flow components

2. **Interactive Interface**:
   - **Drag & Drop**: Click and drag components to reposition them
   - **Connection Creation**: Click and drag from output ports (red) to input ports (blue) to create data flow connections
   - **Selection**: Click components to select them (highlighted with gold border)
   - **Debug Mode**: Press 'D' to toggle debug information display
   - **Component Deletion**: Select components and press Delete to remove them

3. **Visual Connections**: 
   - Green arrows show data flow between connected components
   - Real-time visual feedback while creating connections
   - Arrows automatically update when components are moved

4. **Grid Background**: Subtle grid for easier component alignment

## Source and Sink Components (Inspired by Akka Streams)

### Source Types

Sources generate data and have only output ports (no inputs):

1. **Source.single(value)** - Emits a single value then completes
2. **Source.tick(value, interval, max_count)** - Emits value at regular intervals
3. **Source.range(start, end, step)** - Emits a sequence of numbers
4. **Source.from_array(array, count)** - Emits elements from an array
5. **Source.repeat(value, count)** - Repeats a value N times
6. **Source.unfold(generator_fn, state)** - Generates elements using a function
7. **Source.empty()** - Emits nothing (completes immediately)

### Sink Types  

Sinks consume data and have only input ports (no outputs):

1. **Sink.foreach(effect_fn)** - Applies side effect to each element
2. **Sink.fold(fold_fn, initial)** - Reduces elements using accumulator function
3. **Sink.head()** - Takes only the first element then completes
4. **Sink.last()** - Takes only the last element
5. **Sink.collect(capacity)** - Collects all elements into an array
6. **Sink.count()** - Counts number of elements received
7. **Sink.ignore()** - Discards all elements (useful for testing)

### Flow Components

Flow components transform data, having both input and output ports:
- Transform data from one type to another
- Apply processing functions
- Can be chained together

## Building and Running

```bash
# Build the basic visual interface
make visual

# Build the sources & sinks demo
make sources-demo

# Run the basic visual interface
make run-visual

# Run the sources & sinks demo
make run-sources

# Alternative: run directly  
./visual
./sources-demo
```

## How It Works

The visual interface integrates with the existing cstream architecture:

1. **Component Integration**: Each visual component wraps a cstream Component and mirrors its input/output ports
2. **Real Connection**: Visual connections actually connect the underlying cstream data ports
3. **Live Processing**: The cstream components run in their own threads while the visual interface provides real-time monitoring
4. **Interactive Design**: Users can create, modify, and delete component graphs visually

## Example: Sources Demo

The `sources-demo` creates a complex pipeline demonstrating various patterns:

### Data Flow Examples:

1. **Single Value Flow**: 
   - `Source.single(42)` → `Print Numbers Sink`

2. **Tick Processing**: 
   - `Source.tick(10, 1s, 5 times)` → `Multiply x2` → `Sum Sink`

3. **Range Processing**: 
   - `Source.range(1..5)` → `Count Sink`
   - `Source.range(1..5)` → `Number to String` → `Print Strings Sink`

4. **Array Processing**:
   - `Source.from_array([100,200,300,400,500])` → `Head Sink`

### Component Types Visual Guide:

- **Green Boxes (Sources)**: Triangular icon, generate data
  - Single(42), Tick(10, 1s), Range(1..5), Array[5]
- **Pink Boxes (Sinks)**: Square icon, consume data  
  - Print Numbers, Sum, Count, Head, Print Strings
- **Gray Boxes (Flow)**: Diamond icon, transform data
  - Multiply x2, To String

## Architecture

- **VisualEditor**: Main editor class managing components and connections
- **VisualComponent**: Visual wrapper around cstream Component with rendering info and type information
- **VisualPort**: Visual representation of component input/output ports
- **Connection**: Visual and logical connection between component ports
- **SourceConfig/SinkConfig**: Configuration structures for different source/sink types

## Interactive Controls

- **Mouse**: 
  - Left click and drag to move components
  - Click output port → drag to input port to create connections
  - Click empty space to deselect
- **Keyboard**:
  - `D` - Toggle debug information
  - `Delete` - Remove selected components
  - `ESC` or close window to exit

## API Usage

### Creating Sources
```c
// Single value
NumberData value = {42};
Component* src = Source_single(arena, &value, sizeof(NumberData), "My Source");

// Tick source (emit every 1000ms, 5 times)
Component* tick_src = Source_tick(arena, &value, sizeof(NumberData), 1000, 5, "Ticker");

// Range source
Component* range_src = Source_range(arena, 1, 10, 1, "Range 1-9");
```

### Creating Sinks
```c
// Print sink
Component* print_sink = Sink_foreach(arena, my_print_function, NULL, sizeof(NumberData), "Printer");

// Sum sink
NumberData initial = {0};
Component* sum_sink = Sink_fold(arena, sum_function, &initial, sizeof(NumberData), sizeof(NumberData), "Sum");

// Count sink
Component* count_sink = Sink_count(arena, sizeof(NumberData), "Counter");
```

### Adding to Visual Editor
```c
// Add source component  
int src_id = VisualEditor_add_source_component(editor, src, (Vector2){100, 100}, "Single(42)", SOURCE_SINGLE);

// Add sink component
int sink_id = VisualEditor_add_sink_component(editor, sink, (Vector2){300, 100}, "Print", SINK_FOREACH);

// Connect them
VisualEditor_add_connection(editor, src_id, 0, sink_id, 0);
```

## Console Output

The demos provide rich console output showing:
- Component startup messages
- Data processing results  
- Sink computation results (sums, counts, etc.)
- Component shutdown information

This provides a complete node-based visual programming environment for the cstream component system, making it easy to design, modify, and understand data processing pipelines through direct manipulation rather than code editing, with powerful source and sink abstractions inspired by reactive streaming libraries.