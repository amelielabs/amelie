
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

static void
test_command_unit(TestSuite* self, Str* arg)
{
	// unit <function_name>
	Str name;
	str_arg(arg, &name);
	if (str_empty(&name))
		test_error(self, "unit <name> expected");

	char* name_cstr = strndup(str_of(&name), str_size(&name));
	assert(name_cstr);
	defer(free, name_cstr);

	void* ptr = dlsym(self->dlhandle, name_cstr);
	if (! ptr)
		test_error(self, "unit function not found");
	void (*test_function)(void*) = ptr;

	BufMgr buf_mgr;
	buf_mgr_init(&buf_mgr);
	defer(buf_mgr_free, &buf_mgr);

	Task task;
	task_init(&task);
	defer(task_free, &task);

	int rc;
	rc = task_create_nothrow(&task, "test", test_function, NULL, NULL, NULL, NULL,
	                         &buf_mgr);
	if (rc == -1)
		test_error(self, "unit task create error");

	task_wait(&task);
}

static void
test_command_open(TestSuite* self, Str* arg)
{
	// open <name> [server options]
	Str name;
	str_arg(arg, &name);
	if (str_empty(&name))
		test_error(self, "open <name> expected");

	auto env = test_plan_find_env(&self->plan, &name);
	if (env)
		test_error(self, "env name redefined");
	env = test_env_create(&name);
	test_plan_add_env(&self->plan, env);

	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s/%.*s",
	         str_size(&self->option_result_dir),
	         str_of(&self->option_result_dir),
	         str_size(&name),
	         str_of(&name));

	// start <name> [server options]
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
		"--wal_worker=false",
		"--wal_sync_on_create=false",
		"--wal_sync_on_close=false",
		"--wal_sync_on_write=false",
		"--frontends=1",
		"--backends=1"
	};

	char options[1024];
	str_chomp(arg);
	if (! str_empty(arg))
	{
		snprintf(options, sizeof(options), "--json={%.*s}",
		         str_size(arg), str_of(arg));
		argv[argc] = options;
		argc++;
	}

	int rc = cli_start(&env->cli, argc, argv);
	if (rc == -1)
		test_error(self, "start failed");
}

static void
test_command_close(TestSuite* self, Str* arg)
{
	// close <name>
	Str name;
	str_arg(arg, &name);
	if (str_empty(&name))
		test_error(self, "close <name> expected");

	auto env = test_plan_find_env(&self->plan, &name);
	if (! env)
		test_error(self, "close <ame> not found");

	if (env->sessions)
		test_error(self, "close has active sessions");

	test_plan_del_env(&self->plan, env);
	test_env_free(env);
}

static void
test_command_backup(TestSuite* self, Str* arg)
{
	// backup <name> [options]
	Str name;
	str_arg(arg, &name);
	if (str_empty(&name))
		test_error(self, "backup <name> expected");

	auto env = test_plan_find_env(&self->plan, &name);
	if (env)
		test_error(self, "env name redefined");
	env = test_env_create(&name);
	test_plan_add_env(&self->plan, env);

	// set client home
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%.*s/home", str_size(&self->option_result_dir),
	         str_of(&self->option_result_dir));
	setenv("AMELIE_HOME", path, 1);

	// set base dir
	snprintf(path, sizeof(path), "%.*s/%.*s",
	         str_size(&self->option_result_dir),
	         str_of(&self->option_result_dir),
	         str_size(&name),
	         str_of(&name));

	// backup <path> --json {remote options}
	int   argc = 4;
	char* argv[6] =
	{
		"amelie-test",
		"backup",
		path,
		"--debug=0"
	};

	char options[1024];
	str_chomp(arg);
	if (! str_empty(arg))
	{
		snprintf(options, sizeof(options), "--json={%.*s}",
		         str_size(arg), str_of(arg));
		argv[argc] = options;
		argc++;
	}

	int rc = cli_start(&env->cli, argc, argv);
	if (rc == -1)
		test_error(self, "%start failed");
}

static void
test_command_connect(TestSuite* self, Str* arg)
{
	// connnect <env> <name> <uri> [cafile]
	Str env_name;
	Str name;
	Str uri;
	Str cafile;
	str_arg(arg, &env_name);
	str_arg(arg, &name);
	str_arg(arg, &uri);
	str_arg(arg, &cafile);
	if (str_empty(&env_name) || str_empty(&name) || str_empty(&uri))
		test_error(self, "connect <env> <name> <uri> [cafile] expected");

	str_shrink(&cafile);
	auto env = test_plan_find_env(&self->plan, &env_name);
	if (! env)
		test_error(self, "env does not exists");

	auto session = test_session_create(&name, env);
	test_plan_add_session(&self->plan, session);
	test_session_connect(session, &uri, &cafile);
	self->current_session = session;
}

static void
test_command_disconnect(TestSuite* self, Str* arg)
{
	// disconnect <name>
	Str name;
	str_arg(arg, &name);
	if (str_empty(&name))
		test_error(self, "disconnect <name> expected");

	auto session = test_plan_find_session(&self->plan, &name);
	if (! session)
		test_error(self, "session does not exists");
	test_plan_del_session(&self->plan, session);
	test_session_free(session);
	if (self->current_session == session)
		self->current_session = NULL;
}

static void
test_command_post(TestSuite* self, Str* arg)
{
	// post <path> <content_type> <content>
	Str path;
	Str content_type;
	str_arg(arg, &path);
	str_arg(arg, &content_type);
	if (str_empty(&path) || str_empty(&content_type))
		test_error(self, "post <path> <content_type> <content> expected");

	test_session_execute(self->current_session, &path, &content_type, arg,
	                     &self->current_test_result);
}

static void
test_command_switch(TestSuite* self, Str* arg)
{
	// switch <name>
	Str name;
	str_arg(arg, &name);
	if (str_empty(&name))
		test_error(self, "switch <name> expected");

	auto session = test_plan_find_session(&self->plan, &name);
	if (! session)
		test_error(self, "session does not exists");
	self->current_session = session;
}

static TestCommand
test_commands[] =
{
	{ "unit",       4,  test_command_unit        },
	{ "open",       4,  test_command_open        },
	{ "close",      5,  test_command_close       },
	{ "backup",     6,  test_command_backup      },
	{ "connect",    7,  test_command_connect     },
	{ "disconnect", 10, test_command_disconnect  },
	{ "post",       4,  test_command_post        },
	{ "switch",     6,  test_command_switch      },
	{  NULL,        0,  NULL                     }
};

void
test_command(TestSuite* self, Str* cmd)
{
	str_chomp(cmd);
	if (str_empty(cmd))
		return;

	// match command
	for (auto i = 0; test_commands[i].name; i++)
	{
		auto command = &test_commands[i];
		if (str_is_prefix(cmd, command->name, command->name_size))
		{
			str_advance(cmd, command->name_size);
			command->fn(self, cmd);
			return;
		}
	}

	// query
	if (! self->current_session)
		test_error(self, "session is not defined");

	// POST /
	Str path;
	str_set_cstr(&path, "/");

	Str content_type;
	str_set_cstr(&content_type, "text/plain");

	test_session_execute(self->current_session, &path, &content_type, cmd,
	                     &self->current_test_result);
}
