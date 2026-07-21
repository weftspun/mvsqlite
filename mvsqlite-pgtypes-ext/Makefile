CFLAGS += -O2 -fPIC -I../mvsqlite-sqlite3

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	SOEXT := dylib
	LDFLAGS += -bundle -flat_namespace -undefined suppress
else
	SOEXT := so
	LDFLAGS += -shared -Wl,-z,noexecstack
endif

SRCS := pgtypes_ext.c uuid.c decimal.c series.c

build: $(SRCS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o libmvsqlite_pgtypes.$(SOEXT) $(SRCS) -lm

clean:
	rm -f libmvsqlite_pgtypes.so libmvsqlite_pgtypes.dylib

.PHONY: build clean
