
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>

enum
{
	SHOW_USERS,
	SHOW_USER,
	SHOW_REPLICAS,
	SHOW_REPLICA,
	SHOW_REPL,
	SHOW_WAL,
	SHOW_METRICS,
	SHOW_DATABASES,
	SHOW_DATABASE,
	SHOW_TABLES,
	SHOW_TABLE,
	SHOW_FUNCTIONS,
	SHOW_FUNCTION,
	SHOW_STATE,
	SHOW_ALL,
	SHOW_CONFIG
};

typedef struct ShowCmd ShowCmd;

struct ShowCmd
{
	int         id;
	const char* name;
	int         name_size;
	bool        has_arg;
};

static ShowCmd show_cmds[] =
{
	{ SHOW_USERS,     "users",       5,  false  },
	{ SHOW_USER,      "user",        4,  true   },
	{ SHOW_REPLICAS,  "replicas",    8,  false  },
	{ SHOW_REPLICA,   "replica",     7,  true   },
	{ SHOW_REPL,      "repl",        4,  false  },
	{ SHOW_REPL,      "replication", 11, false  },
	{ SHOW_WAL,       "wal",         3,  false  },
	{ SHOW_METRICS,   "metrics",     7,  false  },
	{ SHOW_DATABASES, "databases",   9,  false  },
	{ SHOW_DATABASE,  "database",    8,  true   },
	{ SHOW_TABLES,    "tables",      6,  false  },
	{ SHOW_TABLE,     "table",       5,  true   },
	{ SHOW_FUNCTIONS, "functions",   9,  false  },
	{ SHOW_FUNCTION,  "function",    8,  true   },
	{ SHOW_STATE,     "state",       5,  false  },
	{ SHOW_ALL,       "all",         3,  false  },
	{ SHOW_CONFIG,    "config",      6,  false  },
	{ 0,               NULL,         0,  false  }
};

static inline ShowCmd*
show_cmd_find(Str* name)
{
	for (auto i = 0; show_cmds[i].name; i++)
	{
		auto cmd = &show_cmds[i];
		if (str_is(name, cmd->name, cmd->name_size))
			return cmd;
	}
	return NULL;
}

static void
fn_show(Fn* self)
{
	auto db = &self->local->db;
	// [section, name, extended]
	Str  section_none;
	Str* section  = NULL;
	Str* name     = NULL;
	bool extended = false;
	switch (self->argc) {
	case 0:
		section = &section_none;
		break;
	case 1:
		// [section]
		fn_expect_arg(self, 0, TYPE_STRING);
		section = &self->argv[0].string;
		break;
	case 2:
		// [section, name | bool]
		fn_expect_arg(self, 0, TYPE_STRING);
		section = &self->argv[0].string;
		if (self->argv[1].type == TYPE_STRING)
			name = &self->argv[1].string;
		else
		if (self->argv[1].type == TYPE_BOOL)
			extended = self->argv[1].integer;
		else
			fn_error_arg(self, 1, "string or bool expected");
		break;
	case 3:
		// [section, name, bool]
		fn_expect_arg(self, 0, TYPE_STRING);
		fn_expect_arg(self, 1, TYPE_STRING);
		fn_expect_arg(self, 2, TYPE_BOOL);
		section  = &self->argv[0].string;
		name     = &self->argv[1].string;
		extended = self->argv[2].integer;
		break;
	default:
		fn_error_noargs(self, "invalid number of arguments");
		break;
	}
	str_set(&section_none, "all", 3);

	// match command
	Buf* buf = NULL;
	auto cmd = show_cmd_find(section);
	if (! cmd)
	{
		// config option
		if (str_empty(section))
			fn_error_noargs(self, "section name is not defined");
		if (name && !str_empty(name))
			fn_error_noargs(self, "unexpected name argument");

		auto opt = opts_find(&config()->opts, section);
		if (opt && opt_is(opt, OPT_S))
			opt = NULL;
		if (unlikely(opt == NULL))
			fn_error_noargs(self, "option '%.*s' is not found", str_size(section),
			                  str_of(section));
		buf = buf_create();
		local_encode_opt(self->local, buf, opt);
		value_set_json_buf(self->result, buf);
		return;
	}

	// ensure argument is set
	if (cmd->has_arg) {
		if (!name || str_empty(name))
			fn_error_noargs(self, "name is missing for '%.*s'",
			                str_size(section),
			                str_of(section));
	} else {
		if (name && !str_empty(name))
			fn_error_noargs(self, "unexpected name argument");
	}

	auto catalog = &share()->storage->catalog;
	switch (cmd->id) {
	case SHOW_USERS:
	{
		buf = user_mgr_list(share()->user_mgr, NULL);
		break;
	}
	case SHOW_USER:
	{
		buf = user_mgr_list(share()->user_mgr, name);
		break;
	}
	case SHOW_REPLICAS:
	{
		buf = replica_mgr_list(&share()->repl->replica_mgr, NULL);
		break;
	}
	case SHOW_REPLICA:
	{
		Uuid id;
		uuid_set(&id, name);
		buf = replica_mgr_list(&share()->repl->replica_mgr, &id);
		break;
	}
	case SHOW_REPL:
	{
		buf = repl_status(share()->repl);
		break;
	}
	case SHOW_WAL:
	{
		buf = wal_status(&share()->storage->wal_mgr.wal);
		break;
	}
	case SHOW_METRICS:
	{
		rpc(&runtime()->task, MSG_SHOW_METRICS, 1, &buf);
		break;
	}
	case SHOW_DATABASES:
	{
		buf = db_mgr_list(&catalog->db_mgr, NULL, extended);
		break;
	}
	case SHOW_DATABASE:
	{
		buf = db_mgr_list(&catalog->db_mgr, name, extended);
		break;
	}
	case SHOW_TABLES:
	{
		buf = table_mgr_list(&catalog->table_mgr, db, NULL, extended);
		break;
	}
	case SHOW_TABLE:
	{
		buf = table_mgr_list(&catalog->table_mgr, db, name, extended);
		break;
	}
	case SHOW_FUNCTIONS:
	{
		buf = udf_mgr_list(&catalog->udf_mgr, db, NULL, extended);
		break;
	}
	case SHOW_FUNCTION:
	{
		buf = udf_mgr_list(&catalog->udf_mgr, db, name, extended);
		break;
	}
	case SHOW_STATE:
	{
		buf = storage_state(share()->storage);
		break;
	}
	case SHOW_ALL:
	case SHOW_CONFIG:
	{
		buf = buf_create();
		encode_obj(buf);
		list_foreach(&config()->opts.list)
		{
			auto opt = list_at(Opt, link);
			if (opt_is(opt, OPT_H) || opt_is(opt, OPT_S))
				continue;
			encode_string(buf, &opt->name);
			local_encode_opt(self->local, buf, opt);
		}
		encode_obj_end(buf);
		break;
	}
	default:
		abort();
	}

	value_set_json_buf(self->result, buf);
}

void
fn_system_register(FunctionMgr* self)
{
	// show()
	auto func = function_allocate(TYPE_JSON, "show", fn_show);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);
}
