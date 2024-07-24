#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Test        Test;
typedef struct TestGroup   TestGroup;
typedef struct TestEnv     TestEnv;
typedef struct TestSession TestSession;
typedef struct TestSuite   TestSuite;

struct Test
{
	char*      name;
	char*      description;
	TestGroup* group;
	List       link_group;
	List       link;
};

struct TestGroup
{
	char* name;
	char* directory;
	List  list_test;
	List  link;
};

struct TestEnv
{
	char* name;
	Main  main;
	int   sessions;
	List  link;
};

struct TestSession
{
	char*    name;
	CURL*    handle;
	void*    headers;
	TestEnv* env;
	List     link;
};

struct TestSuite
{
	void*        dlhandle;
	char*        current_test_options;
	char*        current_test;
	FILE*        current_test_result;
	int          current_test_ok_exists;
	char*        current_test_ok_file;
	char*        current_test_result_file;
	int          current_test_started;
	int          current_line;
	int          current_plan_line;
	TestGroup*   current_group;
	Test*        current;
	TestSession* current_session;
	List         list_env;
	List         list_session;
	char*        option_test_dir;
	char*        option_result_dir;
	char*        option_plan_file;
	char*        option_test;
	char*        option_group;
	int          option_fix;
	List         list_test;
	List         list_group;
};

void test_suite_init(TestSuite*);
void test_suite_free(TestSuite*);
void test_suite_cleanup(TestSuite*);
int  test_suite_run(TestSuite*);

static inline void
test_info(TestSuite* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vprintf(fmt, args);
	va_end(args);
	fflush(stdout);
}

static inline void
test_error(TestSuite* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	printf("\n");
	printf("error: ");
	vprintf(fmt, args);
	printf("\n");
	va_end(args);
	fflush(stdout);
}

static inline void
test_log(TestSuite* self, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vfprintf(self->current_test_result, fmt, args);
	va_end(args);
	fflush(self->current_test_result);
}

static inline int
test_sh(TestSuite* self, const char* fmt, ...)
{
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
