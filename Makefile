main: main.c cstream.h
	cc -std=c11 -o main main.c -lpthread

dbg: main.c
	# cc -v
	cc -std=c11 -Wall -Wextra -pedantic -fsanitize=address -o main main.c -lpthread -g -O0

test: test.c cstream.h
	cc -std=c11 -Wall -Wextra -pedantic -fsanitize=address -o test test.c -lpthread -g -O0

compile_commands.json: Makefile
	bear -- make -B main

run: main
	./main

run-test: test
	./test

clean:
	rm -f main compile_commands.json test

.PHONY: run clean main-dbg dbg
