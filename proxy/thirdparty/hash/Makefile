CC      := gcc
LD      := ld
CFLAGS  := -fpic 
LIB     := 
LDFLAGS :=  -shared
SOURCE  := $(wildcard *.c)
OBJS    := $(patsubst %.c,%.o,$(SOURCE))
TARGET_LIB := libhash.so

all:$(OBJS)
	$(CC) $(LDFLAGS) $(LIB) -o $(TARGET_LIB) $(OBJS) 
%.o:%.c
	$(CC) -c $(CFLAGS)  $< -o $*.o
clean:
	rm *.so *.o -rf
