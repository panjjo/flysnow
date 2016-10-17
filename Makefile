NY: all

all: | $(log)
	./parser
	go build main.go
	./main

