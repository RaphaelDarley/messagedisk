CXXFLAGS=-Wall -Werror -O -I nbdkit/include -I nbdkit/common/include -I nbdkit/common/allocators -I nbdkit/common/utils -fPIC -DPSS_STYLE=2 -shared

main:
	$(CC) $(wildcard src/*.c) -o bin/blockmerchant.so -I include -L lib $(CXXFLAGS)

all: main

clean:
	rm bin/*