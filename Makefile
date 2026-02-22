main: main.c cstream.h
	cc -std=c11 -o main main.c -lpthread

dbg: main.c
	# cc -v
	cc -std=c11 -Wall -Wextra -pedantic -fsanitize=address -o main main.c -lpthread -g -O0

test: test.c cstream.h
	cc -std=c11 -Wall -Wextra -pedantic -fsanitize=address -o test test.c -lpthread -g -O0

plugin_example.so: plugin_example.c cstream.h
	cc -std=c11 -Wall -Wextra -pedantic -shared -fPIC -o plugin_example.so plugin_example.c -lpthread

test_plugin: test_plugin.c cstream.h plugin_example.so
	cc -std=c11 -Wall -Wextra -pedantic -fsanitize=address -o test_plugin test_plugin.c -lpthread -ldl -g -O0

test_plugin_reload: test_plugin_reload.c cstream.h
	cc -std=c11 -Wall -Wextra -pedantic -fsanitize=address \
	   -DCSTREAM_HEADER_DIR=\"$(shell pwd)\" \
	   -o test_plugin_reload test_plugin_reload.c -lpthread -ldl -g -O0

compile_commands.json: Makefile
	bear -- make -B main

run: main
	./main

run-test: test
	./test

run-test-plugin: test_plugin
	./test_plugin

run-test-plugin-reload: test_plugin_reload
	./test_plugin_reload

clean:
	rm -f main compile_commands.json test test_plugin test_plugin_reload plugin_example.so

.PHONY: run clean main-dbg dbg
