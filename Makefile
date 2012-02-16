INCS = -I /usr/local/include 
CFLAGS = -undefined dynamic_lookup -dynamiclib -DOCI_IMPORT_LINKAGE -DOCI_CHARSET_MIXED
LDFLAGS= -L/usr/local/oracle/instantclient_10_2/ -lclntsh  -L/usr/local/lib -locilib -L/usr/local/Cellar/erlang/R14B04/lib/erlang/usr/include/
CC = gcc
SRCS = prophet
OBJS = $(SRCS:.c=.so)

all: default 
  
default:
  gcc -undefined dynamic_lookup -dynamiclib -DOCI_IMPORT_LINKAGE -DOCI_CHARSET_MIXED -L/usr/local/oracle/instantclient_10_2/ -lclntsh -L/usr/local/lib -locilib -L/usr/local/Cellar/erlang/R14B04/lib/erlang/usr/include/ prophet.c -o prophet.so

demo: $(OBJS)
	$(CC) $(LDFLAGS) -o prophet.so

clean: 
	rm -f *~ $(OBJS)

