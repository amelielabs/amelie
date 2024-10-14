all: build/CMakeCache.txt
	@(cd build && make --no-print-directory)
build/CMakeCache.txt:
	@(cd build && cmake -DCMAKE_BUILD_TYPE=Debug .)
debug:
	@(cd build && cmake -DCMAKE_BUILD_TYPE=Debug .)
	@(echo)
	@(cd build && make --no-print-directory)
release:
	@(cd build && cmake -DCMAKE_BUILD_TYPE=Release .)
	@(echo)
	@(cd build && make --no-print-directory)
clean:
	@(cd build && make clean --no-print-directory)
distclean:
	git clean -f -x -d
