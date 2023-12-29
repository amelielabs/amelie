all:
	@(cd indigo; make --no-print-directory)
	@(cd test;   make --no-print-directory)
	@(cd cli;    make --no-print-directory)
clean:
	@(cd indigo; make --no-print-directory clean)
	@(cd test;   make --no-print-directory clean)
	@(cd cli;    make --no-print-directory clean)
