#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

typedef struct TestSuite TestSuite;

struct TestSuite
{
	char*        current_test_file;
	File         current_test_result;
	int          current_test_ok_exists;
	char*        current_test_ok_file;
	char*        current_test_result_file;
	bool         current_test_started;
	int          current_line;
	TestGroup*   current_group;
	Test*        current;
	TestSession* current_session;
	bool         option_fix;
	Str          option_test_dir;
	Str          option_result_dir;
	Str          option_plan_file;
	Str          option_test;
	Str          option_group;
	void*        dlhandle;
	TestPlan     plan;
};

void test_suite_init(TestSuite*);
void test_suite_free(TestSuite*);
void test_suite_cleanup(TestSuite*);
void test_suite_run(TestSuite*);

static inline void
test_log(TestSuite* self, Str* data)
{
	if (str_empty(data))
		return;
	file_write(&self->current_test_result, data->pos, str_size(data));
	file_write(&self->current_test_result, "\n", 1);
}

static inline void
test_error(TestSuite* self, const char* msg)
{
	error("%s:%d: %s\n", self->current_test_file,
	      self->current_line, msg);
}

static inline int
test_sh(TestSuite* self, const char* fmt, ...)
{
	unused(self);
	va_list args;
	va_start(args, fmt);
	char cmd[PATH_MAX];
	vsnprintf(cmd, sizeof(cmd), fmt, args);
	int rc = system(cmd);
	va_end(args);
	return rc;
}

#define test(expr) ({ \
	if (! (expr)) { \
		fprintf(stdout, "fail (%s:%d) %s\n", __FILE__, __LINE__, #expr); \
		fflush(stdout); \
		abort(); \
	} \
})
