CURL_FLAGS := $(shell curl-config --cflags --libs)
CXXFLAGS := -I nbdkit/include -I nbdkit/common/include -I nbdkit/common/allocators -I nbdkit/common/utils -fPIC -DPSS_STYLE=2 -shared

main:
	$(CC) $(wildcard src/*.c) -o bin/blockmerchant.so -I include -L lib $(CXXFLAGS) $(CURL_FLAGS)

all: main

clean:
	rm bin/*