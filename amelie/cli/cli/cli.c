
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

extern void cli_cmd_init(Cli*, int, char**);
extern void cli_cmd_start(Cli*, int, char**);
extern void cli_cmd_stop(Cli*, int, char**);
extern void cli_cmd_backup(Cli*, int, char**);
extern void cli_cmd_login(Cli*, int, char**);
extern void cli_cmd_logout(Cli*, int, char**);
extern void cli_cmd_client(Cli*, int, char**);
extern void cli_cmd_import(Cli*, int, char**);
extern void cli_cmd_top(Cli*, int, char**);
extern void cli_cmd_bench(Cli*, int, char**);
extern void cli_cmd_test(Cli*, int, char**);

static struct CliCmd
cli_commands[] =
{
	// server
	{ cli_cmd_init,   "init",   "Create a database repository"        },
	{ cli_cmd_start,  "start",  "Start a server"                      },
	{ cli_cmd_stop,   "stop",   "Stop a server"                       },
	{ cli_cmd_backup, "backup", "Create backup of a remote server"    },
	{ cli_cmd_login,  "login",  "Create or update login information"  },
	{ cli_cmd_logout, "logout", "Delete login information"            },
	{ cli_cmd_client, "client", "Connect to a remote server"          },
	{ cli_cmd_import, "import", "Import data to a remote server"      },
	{ cli_cmd_top,    "top",    "Get information about remote server" },
	{ cli_cmd_bench,  "bench",  "Run benchmarks on a server"          },
	{ cli_cmd_test,   "test",   "Run tests"                           },
	{ NULL,            NULL,     NULL                                 },
};

static void
cli_usage(void)
{
	auto version = &state()->version.string;
	info("amelie (version: %.*s)", str_size(version), str_of(version));
	info("");
	info("Usage: amelie [COMMAND | LOGIN] [OPTIONS]");
	info("");
	info("Commands:");
	info("");
	for (auto i = 0;; i++)
	{
		auto cmd = &cli_commands[i];
		if (! cmd->name)
			break;
		info("  %-10s %s", cmd->name, cmd->description);
	}
	info("");
}

static void
cli_main(Cli* self, int argc, char** argv)
{
	// amelie [command] [options]
	if (argc <= 1 || !strcmp(argv[1], "-h") || !strcmp(argv[1], "--help"))
	{
		cli_usage();
		return;
	}
	if (!strcmp(argv[1], "-v") ||
	    !strcmp(argv[1], "--version"))
	{
		auto version = &state()->version.string;
		info("%.*s", str_size(version), str_of(version));
		return;	
	}

	for (auto i = 0;; i++)
	{
		auto cmd = &cli_commands[i];
		if (! cmd->name)
			break;
		if (strcmp(cmd->name, argv[1]) != 0)
			continue;
		cmd->function(self, argc - 2, argv + 2);
		return;
	}

	// client by default
	cli_cmd_client(self, argc - 1, argv + 1);
}

typedef struct
{
	int    argc;
	char** argv;
	Cli*   self;
} CliArgs;

static void
cli_runner(void* arg)
{
	CliArgs* args = arg;
	Cli* self = args->self;

	auto on_error = error_catch
	(
		instance_start(&self->instance);
		cli_main(self, args->argc, args->argv);
	);
	instance_stop(&self->instance);

	// complete
	CliRc rc = CLI_COMPLETE;
	if (on_error)
		rc = CLI_ERROR;
	cond_signal(&self->task.status, rc);
}

void
cli_init(Cli* self)
{
	home_init(&self->home);
	instance_init(&self->instance);
	task_init(&self->task);
}

void
cli_free(Cli* self)
{
	task_free(&self->task);
	home_free(&self->home);
	instance_free(&self->instance);
}

CliRc
cli_start(Cli* self, int argc, char** argv)
{
	// start cli task
	CliArgs args =
	{
		.argc = argc,
		.argv = argv,
		.self = self
	};
	int rc;
	rc = task_create_nothrow(&self->task, "main", cli_runner, &args,
	                         &self->instance.global,
	                         logger_write, &self->instance.logger,
	                         &self->instance.buf_mgr);
	if (unlikely(rc == -1))
		return CLI_ERROR;

	// wait for cli task to start
	rc = cond_wait(&self->task.status);
	return rc;
}

void
cli_stop(Cli* self)
{
	auto buf = msg_create_as(&self->instance.buf_mgr, RPC_STOP, 0);
	channel_write(&self->task.channel, buf);
	task_wait(&self->task);
}
