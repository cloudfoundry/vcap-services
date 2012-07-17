EXECUTABLE = dpxfs

OBJS = $(patsubst %.c,%.o,$(wildcard *.c))
CFLAGS = -D_FILE_OFFSET_BITS=64 -I../packages/fuse/include/fuse -I../packages/hiredis/include/hiredis
LDFLAGS = -pthread -lfuse -lhiredis -L../packages/hiredis/lib -L../packages/fuse/lib
DEBUG = -g
BINDIR = ../bin

all: bin
	mkdir -p $(BINDIR)
	cp $(EXECUTABLE) $(BINDIR)

bin: $(OBJS)
	$(CC) -o $(EXECUTABLE) $(OBJS) $(LDFLAGS)

%.o: %.c
	$(CC) -Wall -c $(CFLAGS) $< -o $@

debug: CFLAGS += $(DEBUG)
debug: all

clean:
	rm -rf $(EXECUTABLE) $(OBJS) $(BINDIR)
