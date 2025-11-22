
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

#include <amelie_core.h>
#include <amelie_cli.h>

typedef struct CliCmd CliCmd;

struct CliCmd
{
	void       (*function)(int, char**);
	const char*  name;
	const char*  description;
};

extern void cli_cmd_init(int, char**);
extern void cli_cmd_start(int, char**);
extern void cli_cmd_stop(int, char**);
extern void cli_cmd_backup(int, char**);
extern void cli_cmd_login(int, char**);
extern void cli_cmd_logout(int, char**);
extern void cli_cmd_client(int, char**);
extern void cli_cmd_import(int, char**);
extern void cli_cmd_top(int, char**);
extern void cli_cmd_bench(int, char**);
extern void cli_cmd_test(int, char**);
extern void cli_cmd_test_slt(int, char**);

static struct CliCmd
cli_commands[] =
{
	// server
	{ cli_cmd_init,     "init",     "Create a database repository"        },
	{ cli_cmd_start,    "start",    "Start a server"                      },
	{ cli_cmd_stop,     "stop",     "Stop a server"                       },
	{ cli_cmd_backup,   "backup",   "Create backup of a remote server"    },
	{ cli_cmd_login,    "login",    "Create or update login information"  },
	{ cli_cmd_logout,   "logout",   "Delete login information"            },
	{ cli_cmd_client,   "client",   "Connect to a remote server"          },
	{ cli_cmd_import,   "import",   "Import data to a remote server"      },
	{ cli_cmd_top,      "top",      "Get information about remote server" },
	{ cli_cmd_bench,    "bench",    "Run benchmarks on a server"          },
	{ cli_cmd_test,     "test",     "Run tests"                           },
	{ cli_cmd_test_slt, "test_slt", "Run slt tests"                       },
	{ NULL,              NULL,       NULL                                 },
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

void
cli_main(char* directory, int argc, char** argv)
{
	unused(directory);

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
		cmd->function(argc - 2, argv + 2);
		return;
	}

	// client by default
	cli_cmd_client(argc - 1, argv + 1);
}
