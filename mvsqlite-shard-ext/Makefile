CFLAGS += -O2 -fPIC -I../mvsqlite-sqlite3

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	SOEXT := dylib
	LDFLAGS += -bundle -flat_namespace -undefined suppress
else
	SOEXT := so
	LDFLAGS += -shared -Wl,-z,noexecstack
endif

build: shard_ext.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o libmvsqlite_shard.$(SOEXT) shard_ext.c

clean:
	rm -f libmvsqlite_shard.so libmvsqlite_shard.dylib

.PHONY: build clean
