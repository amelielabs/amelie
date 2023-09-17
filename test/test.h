#pragma once

//
// monotone
//
// SQL OLTP database
//

#define test(expr) \
	do { \
		if (! (expr)) { \
			fprintf(stdout, "fail (%s:%d) %s\n", __FILE__, __LINE__, #expr); \
			fflush(stdout); \
			abort(); \
		} \
	} while (0)

#define test_run_function(function) \
	do { \
		fprintf(stdout, "%s: ", #function); \
		(function)(); \
		fprintf(stdout, "ok\n"); \
	} while (0)

#define test_run(function) test_runner(#function, function)
