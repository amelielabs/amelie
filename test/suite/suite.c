
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

#include <amelie_private.h>
#include <amelie_test.h>
#include <dlfcn.h>

static Test*
test_new(TestSuite*  self, TestGroup* group,
         const char* name,
         const char* description)
{
	Test* test = malloc(sizeof(*test));
	if (group == NULL)
		return NULL;
	test->name = strdup(name);
	if (test->name == NULL) {
		free(test);
		return NULL;
	}
	test->description = NULL;
	if (description) {
		test->description = strdup(description);
		if (test->description == NULL) {
			free(test->name);
			free(test);
			return NULL;
		}
	}
	test->group = group;
	list_init(&test->link_group);
	list_init(&test->link);
	list_append(&group->list_test, &test->link_group);
	list_append(&self->list_test, &test->link);
	return test;
}

static void
test_free(Test* test)
{
	free(test->name);
	if (test->description)
		free(test->description);
	free(test);
}

static Test*
test_find(TestSuite* self, const char* name)
{
	list_foreach(&self->list_test)
	{
		auto test = list_at(Test, link);
		if (strcasecmp(name, test->name) == 0)
			return test;
	}
	return NULL;
}

static TestGroup*
test_group_new(TestSuite*  self,
               const char* name,
               const char* directory)
{
	TestGroup* group = malloc(sizeof(*group));
	if (group == NULL)
		return NULL;
	group->name = strdup(name);
	if (group->name == NULL) {
		free(group);
		return NULL;
	}
	group->directory = strdup(directory);
	if (group->directory == NULL) {
		free(group->name);
		free(group);
		return NULL;
	}
	list_init(&group->list_test);
	list_init(&group->link);
	list_append(&self->list_group, &group->link);
	return group;
}

static void
test_group_free(TestGroup* group)
{
	list_foreach_safe(&group->list_test)
	{
		auto test = list_at(Test, link_group);
		test_free(test);
	}
	free(group->name);
	free(group->directory);
	free(group);
}

static TestGroup*
test_group_find(TestSuite* self, const char* name)
{
	list_foreach(&self->list_group)
	{
		auto group = list_at(TestGroup, link);
		if (strcasecmp(name, group->name) == 0)
			return group;
	}
	return NULL;
}

static TestEnv*
test_env_new(TestSuite* self, const char* name)
{
	TestEnv* env = malloc(sizeof(*env));
	if (env == NULL)
		return NULL;
	env->name = strdup(name);
	if (env->name == NULL) {
		free(env);
		return NULL;
	}
	env->sessions = 0;
	amelie_init(&env->main);
	list_init(&env->link);
	list_append(&self->list_env, &env->link);
	return env;
}

static void
test_env_free(TestSuite* self, TestEnv* env)
{
	(void)self;
	list_unlink(&env->link);
	amelie_stop(&env->main);
	amelie_free(&env->main);
	free(env->name);
	free(env);
}

static TestEnv*
test_env_find(TestSuite* self, const char* name)
{
	list_foreach(&self->list_env)
	{
		auto env = list_at(TestEnv, link);
		if (strcasecmp(name, env->name) == 0)
			return env;
	}
	return NULL;
}

static char*
test_suite_chomp(char* start)
{
	if (start == NULL)
		return NULL;
	char* pos = start;
	while (*pos != '\n')
		pos++;
	*pos = 0;
	return start;
}

static char*
test_suite_arg(char** start)
{
	if (*start == NULL)
		return NULL;
	char* pos = *start;
	while (*pos == ' ')
		pos++;
	char* arg = pos;
	while (*pos != '\n' && *pos != ' ')
		pos++;
	int is_eol = *pos == '\n';
	*pos = 0;
	pos++;
	*start = (is_eol) ? NULL : pos;
	return arg;
}

static int
test_suite_plan_group(TestSuite* self, char* arg, char* directory)
{
	char* name = test_suite_arg(&arg);
	if (name == NULL || !strlen(name)) {
		test_error(self, "line %d: plan: bad group name",
		           self->current_plan_line);
		return -1;
	}
	auto group = test_group_find(self, name);
	if (group) {
		test_error(self, "line %d: plan: group '%s' redefined",
		           self->current_plan_line,
		           name);
		return -1;
	}
	group = test_group_new(self, name, directory);
	self->current_group = group;
	return 0;
}

static int
test_suite_plan_test(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	char* description = test_suite_chomp(arg);

	if (!strlen(name)) {
		test_error(self, "line %d: plan: bad test definition",
		           self->current_plan_line);
		return -1;
	}

	if (self->current_group == NULL) {
		test_error(self, "line %d: plan: test group is not defined for test",
		           self->current_plan_line);
		return -1;
	}

	auto test = test_find(self, name);
	if (test) {
		test_error(self, "line %d: plan: test '%s' redefined",
		           self->current_plan_line,
		           name);
		return -1;
	}

	test_new(self, self->current_group, name, description);
	return 0;
}

static int
test_suite_plan(TestSuite* self)
{
	FILE* file = fopen(self->option_plan_file, "r");
	if (! file) {
		test_error(self, "plan: failed to open plan file: %s",
		           self->option_plan_file);
		goto error;
	}
	self->current_plan_line = 1;

	char directory[PATH_MAX];
	memset(directory, 0, sizeof(directory));

	char query[PATH_MAX];
	int  rc;
	for (;; self->current_plan_line++)
	{
		if (! fgets(query, sizeof(query), file))
			break;

		if (*query == '\n' || *query == '#')
			continue;

		if (strncmp(query, "directory", 9) == 0)
		{
			// directory <name>
			char* arg = query + 9;
			char* name = test_suite_arg(&arg);
			snprintf(directory, sizeof(directory), "%s", name);
		} else
		if (strncmp(query, "group", 5) == 0)
		{
			if (strlen(directory) == 0)
			{
				test_error(self, "plan: %d: directory is not set",
				           self->current_plan_line);
				goto error;
			}
			// group <name>
			rc = test_suite_plan_group(self, query + 5, directory);
			if (rc == -1)
				goto error;
		} else
		if (strncmp(query, "test", 4) == 0)
		{
			// test <name> <description>
			rc = test_suite_plan_test(self, query + 4);
			if (rc == -1)
				goto error;
		} else
		{
			test_error(self, "plan: %d: unknown plan file command",
			           self->current_plan_line);
			goto error;
		}
	}

	fclose(file);
	return 0;

error:
	if (file)
		fclose(file);
	return -1;
}

static int
test_suite_open(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	char* options = arg;
	if (name == NULL)
	{
		test_error(self, "line %d: open <name> expected",
		           self->current_line);
		return -1;
	}

	auto env = test_env_find(self, name);
	if (env) {
		test_error(self, "line %d: env name redefined",
		           self->current_line);
		return -1;
	}

	env = test_env_new(self, name);
	if (env == NULL)
		return -1;

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/%s", self->option_result_dir, name);

	// start <path> [server options]
	int   argc = 13;
	char* argv[16] =
	{
		"amelie-test",
		"start",
		path,
		"--log_enable=true",
		"--log_to_stdout=false",
		"--log_options=true",
		"--timezone=UTC",
		"--shutdown=graceful",
		"--wal_sync_on_rotate=false",
		"--wal_sync_on_shutdown=false",
		"--wal_sync_on_write=false",
		"--frontends=1",
		"--backends=1"
	};

	char options_test[1024];
	if (self->current_test_options)
	{
		snprintf(options_test, sizeof(options_test), "--json={%s}",
		         self->current_test_options);
		argv[argc] = options_test;
		argc++;
	}

	char options_this[1024];
	if (options)
	{
		snprintf(options_this, sizeof(options_this), "--json={%s}",
		         options);
		argv[argc] = options_this;
		argc++;
	}

	int rc = amelie_start(&env->main, argc, argv);
	if (rc == -1)
	{
		test_error(self, "line %d: start failed", self->current_line);
		test_env_free(self, env);
		return -1;
	}

	return 0;
}

static int
test_suite_backup(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	char* options = arg;
	if (name == NULL)
	{
		test_error(self, "line %d: backup <name> expected",
		           self->current_line);
		return -1;
	}

	auto env = test_env_find(self, name);
	if (env) {
		test_error(self, "line %d: env name redefined",
		           self->current_line);
		return -1;
	}

	env = test_env_new(self, name);
	if (env == NULL)
		return -1;

	// set client home
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/home", self->option_result_dir);
	setenv("AMELIE_HOME", path, 1);

	// set base dir
	snprintf(path, sizeof(path), "%s/%s", self->option_result_dir, name);

	// backup <path> --json {remote options}
	int   argc = 4;
	char* argv[6] =
	{
		"amelie-test",
		"backup",
		path,
		"--debug=0"
	};

	char options_this[1024];
	if (options)
	{
		snprintf(options_this, sizeof(options_this), "--json={%s}",
		         options);
		argv[argc] = options_this;
		argc++;
	}

	int rc = amelie_start(&env->main, argc, argv);
	if (rc == -1)
	{
		test_error(self, "line %d: start failed", self->current_line);
		test_env_free(self, env);
		return -1;
	}

	return 0;
}

static int
test_suite_close(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	if (name == NULL)
	{
		test_error(self, "line %d: close <name> expected",
		           self->current_line);
		return -1;
	}

	auto env = test_env_find(self, name);
	if (! env) {
		test_error(self, "line %d: close: env name not found",
		           self->current_line);
		return -1;
	}
	if (env->sessions) {
		test_error(self, "line %d: close: has active sessions",
		           self->current_line);
		return -1;
	}
	test_env_free(self, env);
	return 0;
}

static int
test_suite_connect(TestSuite* self, char* arg)
{
	char* env_name = test_suite_arg(&arg);
	char* name = test_suite_arg(&arg);
	char* uri = test_suite_arg(&arg);

	if (env_name == NULL || name == NULL || uri == NULL)
	{
		test_error(self, "line %d: connect <env_name> <name> <uri> expected",
		           self->current_line);
		return -1;
	}

	auto env = test_env_find(self, env_name);
	if (! env) {
		test_error(self, "line %d: connect: env name not found",
		           self->current_line);
		return -1;
	}

	auto session = test_session_new(self, env, name);
	if (session == NULL)
		return -1;

	test_session_connect(self, session, uri, NULL);
	self->current_session = session;
	return 0;
}

static int
test_suite_connect_tls(TestSuite* self, char* arg)
{
	char* env_name = test_suite_arg(&arg);
	char* name = test_suite_arg(&arg);
	char* uri = test_suite_arg(&arg);
	char* cafile = test_suite_arg(&arg);

	if (env_name == NULL || name == NULL || uri == NULL || cafile == NULL)
	{
		test_error(self, "line %d: connect_tls <env_name> <name> <uri> <cafile> expected",
		           self->current_line);
		return -1;
	}

	auto env = test_env_find(self, env_name);
	if (! env) {
		test_error(self, "line %d: connect_tls: env name not found",
		           self->current_line);
		return -1;
	}

	auto session = test_session_new(self, env, name);
	if (session == NULL)
		return -1;

	test_session_connect(self, session, uri, cafile);
	self->current_session = session;
	return 0;
}

static int
test_suite_disconnect(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	if (name == NULL)
	{
		test_error(self, "line %d: disconnect <name> expected",
		           self->current_line);
		return -1;
	}

	auto session = test_session_find(self, name);
	if (session == NULL) {
		test_error(self, "line %d: disconnect: session %s is not found",
		           self->current_line, name);
		return -1;
	}
	test_session_free(self, session);
	if (self->current_session == session)
		self->current_session = NULL;
	return 0;
}

static int
test_suite_import(TestSuite* self, char* arg)
{
	char* type = test_suite_arg(&arg);
	char* data = arg;
	if (!type || !data)
	{
		test_error(self, "line %d: import <type> <data> expected",
		           self->current_line);
		return -1;
	}
	if (self->current_session == NULL) {
		test_error(self, "%d: import: session is not defined",
		           self->current_line);
		return -1;
	}
	test_session_import(self, self->current_session, type, data);
	return 0;
}

static int
test_suite_switch(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	if (name == NULL)
	{
		test_error(self, "line %d: switch <name> expected",
		           self->current_line);
		return -1;
	}

	auto session = test_session_find(self, name);
	if (session == NULL) {
		test_error(self, "line %d: switch: session %s is not found",
		           self->current_line, name);
		return -1;
	}
	self->current_session = session;
	return 0;
}

static int
test_suite_debug(TestSuite* self, char* arg)
{
	unused(self);
	unused(arg);
	// do nothing, left for breakpoint
	return 0;
}

static int
test_suite_query(TestSuite* self, const char* query)
{
	if (self->current_session == NULL) {
		test_error(self, "%d: query: session is not defined",
		           self->current_line);
		return -1;
	}
	test_session_execute(self, self->current_session, query);
	return 0;
}

static int
test_suite_unit_function(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	if (name == NULL)
	{
		test_error(self, "line %d: unit_function <name> expected",
		           self->current_line);
		return -1;
	}

	void* ptr = dlsym(self->dlhandle, name);
	if (ptr == NULL)
	{
		test_error(self, "unit: test '%s' function not found\n", name);
		return -1;
	}
	void (*test_function)(void) = ptr;
	test_function();
	return 0;
}

static int
test_suite_unit(TestSuite* self, char* arg)
{
	char* name = test_suite_arg(&arg);
	if (name == NULL)
	{
		test_error(self, "line %d: unit <name> expected",
		           self->current_line);
		return -1;
	}

	void* ptr = dlsym(self->dlhandle, name);
	if (ptr == NULL)
	{
		test_error(self, "unit: test '%s' function not found\n", name);
		return -1;
	}
	void (*test_function)(void*) = ptr;

	BufMgr buf_mgr;
	buf_mgr_init(&buf_mgr);

	Task task;
	task_init(&task);

	int rc;
	rc = task_create_nothrow(&task, name, test_function, NULL, NULL, NULL, NULL,
	                         &buf_mgr);
	if (rc == -1)
	{
		test_error(self, "unit: test '%s' task create error\n", name);
		return -1;
	}

	task_wait(&task);
	task_free(&task);

	buf_mgr_free(&buf_mgr);
	return 0;
}

static int
test_suite_test_check(TestSuite* self)
{
	// new test
	if (! self->current_test_ok_exists)
	{
		test_info(self, "\033[1;32mnew\033[0m\n");
		return 0;
	}

	// diff
	struct stat result_stat;
	stat(self->current_test_result_file, &result_stat);
	int rc;
	rc = test_sh(self, "cmp -s -n %d %s %s", result_stat.st_size,
	             self->current_test_result_file,
	             self->current_test_ok_file);
	if (rc == 0)
	{
		test_info(self, "%s", "\n");
		return 0;
	}

	if (self->option_fix) {
		test_info(self, "%s", "\033[1;35mfixed\033[0m\n");
	} else {
		test_info(self, "%s", "\033[1;31mfailed\033[0m\n");
	}
	return -1;
}

static int
test_suite_test(TestSuite* self, char* description)
{
	// finish previous test
	int rc = 0;
	if (self->current_test_started)
		rc = test_suite_test_check(self);
	else
		self->current_test_started = 1;

	// start new test
	test_suite_chomp(description);
	/*test_info(self, "  \033[1;30m⚫\033[0m %s ", description);*/
	test_info(self, "    - %s ", description);
	return rc;
}

static int
test_suite_execute(TestSuite* self, Test* test, char* options)
{
	test_sh(self, "mkdir %s", self->option_result_dir, test->name);

	char test_file[PATH_MAX];
	snprintf(test_file, sizeof(test_file), "%s/%s.test",
	         test->group->directory, test->name);

	if (test->description && options)
		test_info(self, "  \033[0;33m%s\033[0m %s: %s\n", test->name, test->description, options);
	else
	if (test->description)
		test_info(self, "  \033[0;33m%s\033[0m %s\n", test->name, test->description);
	else
	if (options)
		test_info(self, "  \033[0;33m%s\033[0m: %s\n", test->name, options);
	else
		test_info(self, "  \033[0;33m%s\033[0m\n", test->name);

	FILE *file;
	file = fopen(test_file, "r");
	if (! file) {
		test_error(self, "test: failed to open test file: %s", test_file);
		return -1;
	}

	char test_result_file[PATH_MAX];
	snprintf(test_result_file, sizeof(test_result_file),
	         "%s.result", test_file);

	char test_ok_file[PATH_MAX];
	snprintf(test_ok_file, sizeof(test_ok_file),
	         "%s.ok", test_file);

	struct stat ok_stat;
	int  rc;
	rc = stat(test_ok_file, &ok_stat);
	self->current_test_ok_exists = rc != -1;
	self->current_test_ok_file = test_ok_file;
	self->current_test_result_file = test_result_file;
	self->current_test = test_file;
	self->current = test;
	self->current_test_options = options;
	self->current_test_started = 0;
	self->current_line = 1;
	self->current_test_result = fopen(test_result_file, "w");
	if (self->current_test_result == NULL) {
		test_error(self, "test: failed to open test result file: %s",
		           test_result_file);
		fclose(file);
		return -1;
	}

	int test_failed = 0;
	char query[16384];
	for (;; self->current_line++)
	{
		if (! fgets(query, sizeof(query), file))
			break;

		if (*query == '\n')
			continue;

		if (*query == '#')
		{
			// test case
			if (strncmp(query, "# test: ", 8) == 0)
			{
				test_log(self, "%s", query);
				rc = test_suite_test(self, query + 8);
				test_failed += (rc == -1);
			}
			continue;
		}

		test_log(self, "%s", query);

		// unit_function
		if (strncmp(query, "unit_function", 13) == 0) {
			rc = test_suite_unit_function(self, query + 13);
			if (rc == -1)
				return -1;
			continue;
		}

		// unit
		if (strncmp(query, "unit", 4) == 0) {
			rc = test_suite_unit(self, query + 4);
			if (rc == -1)
				return -1;
			continue;
		}

		// open
		if (strncmp(query, "open", 4) == 0) {
			rc = test_suite_open(self, query + 4);
			if (rc == -1)
				return -1;
			continue;
		}

		// close
		if (strncmp(query, "close", 5) == 0) {
			rc = test_suite_close(self, query + 5);
			if (rc == -1)
				return -1;
			continue;
		}

		// backup
		if (strncmp(query, "backup", 6) == 0) {
			rc = test_suite_backup(self, query + 6);
			if (rc == -1)
				return -1;
			continue;
		}

		// connect_tls
		if (strncmp(query, "connect_tls", 11) == 0) {
			rc = test_suite_connect_tls(self, query + 11);
			if (rc == -1)
				return -1;
			continue;
		}

		// connect
		if (strncmp(query, "connect", 7) == 0) {
			rc = test_suite_connect(self, query + 7);
			if (rc == -1)
				return -1;
			continue;
		}

		// disconnect
		if (strncmp(query, "disconnect", 10) == 0) {
			rc = test_suite_disconnect(self, query + 10);
			if (rc == -1)
				return -1;
			continue;
		}

		// import
		if (strncmp(query, "import", 6) == 0) {
			rc = test_suite_import(self, query + 6);
			if (rc == -1)
				return -1;
			continue;
		}

		// switch
		if (strncmp(query, "switch", 6) == 0) {
			rc = test_suite_switch(self, query + 6);
			if (rc == -1)
				return -1;
			continue;
		}

		// debug
		if (strncmp(query, "debug", 5) == 0) {
			rc = test_suite_debug(self, query + 5);
			if (rc == -1)
				return -1;
			continue;
		}

		test_suite_chomp(query);

		// query
		rc = test_suite_query(self, query);
		if (rc == -1)
			return -1;
	}

	if (self->current_test_started)
	{
		rc = test_suite_test_check(self);
		test_failed += rc == -1;
	}

	fclose(file);
	fclose(self->current_test_result);
	self->current_test_result = NULL;;

	// create new test
	if (! self->current_test_ok_exists)
		test_sh(self, "cp %s %s", test_result_file, test_ok_file);

	// fix the test
	if (test_failed && self->option_fix) {
		test_sh(self, "cp %s %s", test_result_file, test_ok_file);
		test_failed = 0;
		// one-time effect
		self->option_fix = 0;
	}

	// show diff and stop
	if (test_failed) {
		test_sh(self, "git diff --no-index %s %s", test_ok_file, test_result_file);
		return -1;
	}

	// cleanup
	if (! list_empty(&self->list_session)) {
		test_error(self, "%s", "test: sessions left open");
		return -1;
	}
	if (! list_empty(&self->list_env)) {
		test_error(self, "%s", "test: env left open");
		return -1;
	}
	unlink(test_result_file);

	test_suite_cleanup(self);
	return 0;
}

static int
test_suite_execute_with_options(TestSuite* self, Test* test)
{
	char options_file[PATH_MAX];
	snprintf(options_file, sizeof(options_file), "%s/%s.test.options",
	         test->group->directory, test->name);
	int rc;
	FILE* file = fopen(options_file, "r");
	if (file) {
		char options[512];
		while ((fgets(options, sizeof(options), file)))
		{
			if (*options == '\n' || *options == '#')
				continue;
			char* p = options;
			while (*p != '\n')
				p++;
			*p = 0;
			rc = test_suite_execute(self, test, options);
			if (rc == -1) {
				fclose(file);
				return -1;
			}
		}
		fclose(file);
		return 0;
	}
	return test_suite_execute(self, test, NULL);
}

static int
test_suite_execute_group(TestSuite* self, TestGroup* group)
{
	test_info(self, "%s\n", group->name);

	self->current_group = group;
	list_foreach(&group->list_test)
	{
		auto test = list_at(Test, link_group);
		int rc;
		rc = test_suite_execute_with_options(self, test);
		if (rc == -1)
			return -1;
	}
	return 0;
}

void
test_suite_init(TestSuite* self)
{
	self->dlhandle                 = NULL;
	self->current_test_ok_exists   = 0;
	self->current_test_ok_file     = 0;
	self->current_test_result_file = 0;
	self->current_test_started     = 0;
	self->current_test_options     = NULL;
	self->current_test             = NULL;
	self->current_test_result      = NULL;
	self->current_line             = 0;
	self->current_plan_line        = 0;
	self->current                  = NULL;
	self->current_group            = NULL;
	self->current_session          = NULL;

	self->option_result_dir        = NULL;
	self->option_test              = NULL;
	self->option_group             = NULL;
	self->option_fix               = 0;

	list_init(&self->list_env);
	list_init(&self->list_session);
	list_init(&self->list_test);
	list_init(&self->list_group);
}

void
test_suite_free(TestSuite* self)
{
	list_foreach_safe(&self->list_group)
	{
		auto group = list_at(TestGroup, link);
		test_group_free(group);
	}
	if (self->dlhandle)
	{
		dlclose(self->dlhandle);
		self->dlhandle = NULL;
	}
}

void
test_suite_cleanup(TestSuite* self)
{
	if (self->current_test_result) {
		fclose(self->current_test_result);
		self->current_test_result = NULL;
	}
	self->current = NULL;
	self->current_test_options = NULL;
	self->current_test = NULL;
	self->current_test_result = NULL;
	self->current_line = 0;

	test_sh(self, "rm -rf %s", self->option_result_dir);
}

int
test_suite_run(TestSuite* self)
{
	test_suite_cleanup(self);

	self->dlhandle = dlopen(NULL, RTLD_NOW);

	// read plan file
	int rc;
	rc = test_suite_plan(self);
	if (rc == -1)
		return -1;
	self->current_group = NULL;

	// run selected group
	if (self->option_group)
	{
		auto group = test_group_find(self, self->option_group);
		if (group == NULL) {
			test_error(self, "suite: group is not '%s' defined",
			           self->option_group);
			return -1;
		}
		return test_suite_execute_group(self, group);
	}

	// run selected test
	if (self->option_test)
	{
		auto test = test_find(self, self->option_test);
		if (test == NULL) {
			test_error(self, "suite: test '%s' is not defined",
			           self->option_test);
			return -1;
		}
		return test_suite_execute_with_options(self, test);
	}

	// run all tests groups
	list_foreach(&self->list_group)
	{
		auto group = list_at(TestGroup, link);
		rc = test_suite_execute_group(self, group);
		if (rc == -1)
			return -1;
	}
	return 0;
}
