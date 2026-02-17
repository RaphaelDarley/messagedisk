CURL_FLAGS := $(shell curl-config --cflags --libs)
CXXFLAGS := -I nbdkit/include -I nbdkit/common/include -I nbdkit/common/allocators -I nbdkit/common/utils -fPIC -DPSS_STYLE=2

#test:
#	$(CC) src/test.c -o bin/blocktest -I include -L lib $(CXXFLAGS) $(CURL_FLAGS)

main:
	$(CC) src/main.c -o bin/blockmerchant.so -I include -L lib $(CXXFLAGS) $(CURL_FLAGS) -shared

all: main

clean:
	rm bin/*