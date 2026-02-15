main: main.c
	# cc -std=c11 -fsanitize=address -o main main.c -lpthread -g -O0
	cc -v -std=c11 -o main main.c -lpthread

main-dbg: main.c
	cc -std=c11 -fsanitize=address -o main main.c -lpthread -g -O0

compile_commands.json: Makefile
	bear -- make main

run: main
	./main

dbg: main-dbg
	./main

clean:
	rm -f main compile_commands.json

.PHONY: run clean
