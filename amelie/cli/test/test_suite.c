
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

#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_test.h>
#include <dlfcn.h>

static bool
test_suite_test_validate(TestSuite* self)
{
	// new test
	if (! self->current_test_ok_exists)
	{
		info("\033[1;32mnew\033[0m\n");
		return true;
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
		info("\n");
		return true;
	}
	if (self->option_fix)
		info("%s", "\033[1;35mfixed\033[0m\n");
	else
		info("%s", "\033[1;31mfailed\033[0m\n");
	return false;
}

static bool
test_suite_test(TestSuite* self, Str* description)
{
	// finish previous test
	bool ok = true;
	if (self->current_test_started)
		ok = test_suite_test_validate(self);
	else
		self->current_test_started = true;

	// start new test
	info("    - %.*s ", str_size(description),
	     str_of(description));
	return ok;
}

static bool
test_suite_execute(TestSuite* self, Test* test)
{
	// create test directory
	test_sh(self, "mkdir %.*s", str_size(&self->option_result_dir),
	        str_of(&self->option_result_dir));

	// prepare test file path
	char test_file[PATH_MAX];
	snprintf(test_file, sizeof(test_file),
	         "%.*s/%.*s.test",
	         str_size(&test->group->directory),
	         str_of(&test->group->directory),
	         str_size(&test->name),
	         str_of(&test->name));

	// prepare test result file path
	char test_result_file[PATH_MAX];
	snprintf(test_result_file, sizeof(test_result_file),
	         "%s.result", test_file);

	// prepare test ok file path
	char test_ok_file[PATH_MAX];
	snprintf(test_ok_file, sizeof(test_ok_file),
	         "%s.ok", test_file);

	info("  \033[0;33m%.*s\033[0m\n", str_size(&test->name),
	     str_of(&test->name));

	// read test file
	auto data = file_import("%s", test_file);
	defer_buf(data);

	// create result file
	unlink(test_result_file);
	file_create(&self->current_test_result, test_result_file);
	defer(file_close, &self->current_test_result);

	int failed = 0;
	self->current_test_ok_exists = fs_exists("%s", test_ok_file);
	self->current_test_ok_file = test_ok_file;
	self->current_test_result_file = test_result_file;
	self->current_test_file = test_file;
	self->current = test;
	self->current_test_started = false;
	self->current_line = 1;

	Str text;
	buf_str(data, &text);
	for (; !str_empty(&text); self->current_line++)
	{
		Str cmd;
		str_split(&text, &cmd, '\n');

		// empty line
		if (str_empty(&cmd))
		{
			str_advance(&text, 1);
			continue;
		}
		str_advance(&text, str_size(&cmd));

		// test case or comment
		if (*cmd.pos == '#')
		{
			// test case
			if (str_is_prefix(&cmd, "# test: ", 8))
			{
				Str description = cmd;
				str_advance(&description, 8);
				str_chomp(&description);
				test_log(self, &cmd);
				if (! test_suite_test(self, &description))
					failed++;
			}
			continue;
		}

		test_log(self, &cmd);

		// process command
		test_command(self, &cmd);
	}

	if (self->current_test_started)
		if (! test_suite_test_validate(self))
			failed++;

	// create new test ok file
	if (! self->current_test_ok_exists)
		test_sh(self, "cp %s %s", test_result_file, test_ok_file);

	// fix the test (one-time effect)
	if (failed && self->option_fix)
	{
		test_sh(self, "cp %s %s", test_result_file, test_ok_file);
		failed = 0;
		self->option_fix = 0;
	}

	// show diff and stop
	if (failed)
	{
		test_sh(self, "git diff --no-index %s %s", test_ok_file, test_result_file);
		return false;
	}

	// cleanup
	if (! list_empty(&self->plan.list_session))
		error("%s", "sessions left open\n");

	if (! list_empty(&self->plan.list_env))
		error("%s", "env left open\n");

	unlink(test_result_file);
	test_suite_cleanup(self);
	return true;
}

static bool
test_suite_execute_group(TestSuite* self, TestGroup* group)
{
	info("%.*s\n", str_size(&group->name), str_of(&group->name));
	self->current_group = group;
	list_foreach(&group->list_test)
	{
		auto test = list_at(Test, link_group);
		if (! test_suite_execute(self, test))
			return false;
	}
	return true;
}

void
test_suite_init(TestSuite* self)
{
	self->current_test_ok_exists   = 0;
	self->current_test_ok_file     = 0;
	self->current_test_result_file = 0;
	self->current_test_started     = false;
	self->current_test_file        = NULL;
	self->current_line             = 0;
	self->current                  = NULL;
	self->current_group            = NULL;
	self->current_session          = NULL;
	self->option_fix               = false;
	self->dlhandle                 = NULL;
	file_init(&self->current_test_result);
	str_init(&self->option_test_dir);
	str_init(&self->option_result_dir);
	str_init(&self->option_plan_file);
	str_init(&self->option_test);
	str_init(&self->option_group);
	test_plan_init(&self->plan);
}

void
test_suite_free(TestSuite* self)
{
	if (self->dlhandle)
		dlclose(self->dlhandle);

	test_plan_free(&self->plan);
}

void
test_suite_cleanup(TestSuite* self)
{
	file_close(&self->current_test_result);

	self->current = NULL;
	self->current_group = NULL;
	self->current_test_file = NULL;
	self->current_test_ok_file = NULL;
	self->current_test_result_file = NULL;
	self->current_line = 0;

	if (fs_exists("%.*s", str_size(&self->option_result_dir),
	              str_of(&self->option_result_dir)))
		test_sh(self, "rm -rf %.*s", str_size(&self->option_result_dir),
		        str_of(&self->option_result_dir));
}

void
test_suite_run(TestSuite* self)
{
	test_suite_cleanup(self);

	self->dlhandle = dlopen(NULL, RTLD_NOW);
	if (! self->dlhandle)
		error_system();

	// read plan file
	test_plan_read(&self->plan, &self->option_plan_file);

	// run selected group
	if (! str_empty(&self->option_group))
	{
		auto group = test_plan_find_group(&self->plan, &self->option_group);
		if (! group)
			error("group '%.*s' does not exists\n", str_size(&self->option_group),
			      str_of(&self->option_group));
		test_suite_execute_group(self, group);
		return;
	}

	// run selected test
	if (! str_empty(&self->option_test))
	{
		auto test = test_plan_find(&self->plan, &self->option_test);
		if (! test)
			error("test '%.*s' does not exists\n", str_size(&self->option_test),
			      str_of(&self->option_test));
		test_suite_execute(self, test);
		return;
	}

	// run all tests groups
	list_foreach(&self->plan.list_group)
	{
		auto group = list_at(TestGroup, link);
		if (! test_suite_execute_group(self, group))
			break;
	}
}
