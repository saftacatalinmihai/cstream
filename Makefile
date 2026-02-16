RAYLIB_CFLAGS = $(shell pkg-config --cflags raylib)
RAYLIB_LIBS = $(shell pkg-config --libs raylib)

main: main.c
	# cc -std=c11 -fsanitize=address -o main main.c -lpthread -g -O0
	cc -v -std=c11 -o main main.c -lpthread

main-dbg: main.c
	cc -std=c11 -fsanitize=address -o main main.c -lpthread -g -O0

visual: visual_main.c visual_cstream.c cstream.h visual_cstream.h
	cc -std=c11 -o visual visual_main.c visual_cstream.c $(RAYLIB_CFLAGS) $(RAYLIB_LIBS) -lpthread -lm

visual-dbg: visual_main.c visual_cstream.c cstream.h visual_cstream.h
	cc -std=c11 -fsanitize=address -o visual visual_main.c visual_cstream.c $(RAYLIB_CFLAGS) $(RAYLIB_LIBS) -lpthread -lm -g -O0

sources-demo: sources_demo.c visual_cstream.c cstream.h cstream_sources.h visual_cstream.h
	cc -std=c11 -o sources-demo sources_demo.c visual_cstream.c $(RAYLIB_CFLAGS) $(RAYLIB_LIBS) -lpthread -lm

sources-demo-dbg: sources_demo.c visual_cstream.c cstream.h cstream_sources.h visual_cstream.h
	cc -std=c11 -fsanitize=address -o sources-demo sources_demo.c visual_cstream.c $(RAYLIB_CFLAGS) $(RAYLIB_LIBS) -lpthread -lm -g -O0

interactive-demo: interactive_demo.c visual_cstream.c cstream.h cstream_sources.h visual_cstream.h
	cc -std=c11 -o interactive-demo interactive_demo.c visual_cstream.c $(RAYLIB_CFLAGS) $(RAYLIB_LIBS) -lpthread -lm

interactive-demo-dbg: interactive_demo.c visual_cstream.c cstream.h cstream_sources.h visual_cstream.h
	cc -std=c11 -fsanitize=address -o interactive-demo interactive_demo.c visual_cstream.c $(RAYLIB_CFLAGS) $(RAYLIB_LIBS) -lpthread -lm -g -O0

compile_commands.json: Makefile
	bear -- make main

run-visual: visual
	./visual

run-sources: sources-demo
	./sources-demo

run-interactive: interactive-demo
	./interactive-demo

clean:
	rm -f main visual sources-demo interactive-demo compile_commands.json

.PHONY: run run-visual run-sources run-interactive clean
