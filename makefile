all:
	@(cd monotone; make --no-print-directory)
	@(cd test;     make --no-print-directory)
clean:
	@(cd monotone; make --no-print-directory clean)
	@(cd test;     make --no-print-directory clean)
