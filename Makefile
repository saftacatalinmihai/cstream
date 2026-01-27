main: main.c
	cc -std=c11 -o main main.c -lpthread

compile_commands.json: Makefile
	bear -- make main

run: main
	time ./main

clean:
	rm -f main compile_commands.json

.PHONY: run clean
