
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>
#include <amelie_func.h>

enum
{
	SHOW_REPLICAS,
	SHOW_REPLICA,
	SHOW_REPL,
	SHOW_WAL,
	SHOW_METRICS,
	SHOW_LIMITS,
	SHOW_API,
	SHOW_MEMORY,
	SHOW_DATABASES,
	SHOW_DATABASE,
	SHOW_TABLES,
	SHOW_TABLE,
	SHOW_FUNCTIONS,
	SHOW_FUNCTION,
	SHOW_RELS,
	SHOW_REL,
	SHOW_GRANTS,
	SHOW_INDEXES,
	SHOW_TOOLS,
	SHOW_RESOURCES,
	SHOW_STATE,
	SHOW_ALL,
	SHOW_CONFIG,
	SHOW_LOCKS
};

enum
{
	SHOW_NO,
	SHOW_YES,
	SHOW_MAYBE
};

typedef struct ShowCmd ShowCmd;

struct ShowCmd
{
	int         id;
	const char* section;
	int         section_size;
	int         name;
	bool        obj;
};

static ShowCmd show_cmds[] =
{
	// system
	{ SHOW_REPLICAS,      "replicas",      8,  SHOW_NO,    false },
	{ SHOW_REPLICA,       "replica",       7,  SHOW_YES,   true  },
	{ SHOW_REPL,          "repl",          4,  SHOW_NO,    true  },
	{ SHOW_REPL,          "replication",   11, SHOW_NO,    true  },
	{ SHOW_WAL,           "wal",           3,  SHOW_NO,    true  },
	{ SHOW_METRICS,       "metrics",       7,  SHOW_NO,    true  },
	{ SHOW_LOCKS,         "locks",         5,  SHOW_NO,    false },

	// database
	{ SHOW_LIMITS,        "limits",        6,  SHOW_NO,    true  },
	{ SHOW_API,           "apis",          4,  SHOW_NO,    true  },
	{ SHOW_API,           "api",           3,  SHOW_NO,    true  },
	{ SHOW_MEMORY,        "memory",        6,  SHOW_NO,    true  },

	// relations
	{ SHOW_DATABASES,     "databases",     9,  SHOW_NO,    false },
	{ SHOW_DATABASE,      "database",      8,  SHOW_YES,   true  },
	{ SHOW_DATABASES,     "dbs",           3,  SHOW_NO,    false },
	{ SHOW_DATABASE,      "db",            2,  SHOW_YES,   true  },
	{ SHOW_TABLES,        "tables",        6,  SHOW_NO,    false },
	{ SHOW_TABLE,         "table",         5,  SHOW_YES,   true  },
	{ SHOW_FUNCTIONS,     "functions",     9,  SHOW_NO,    false },
	{ SHOW_FUNCTION,      "function",      8,  SHOW_YES,   true  },
	{ SHOW_RELS,          "rels",          4,  SHOW_NO,    false },
	{ SHOW_REL,           "rel",           3,  SHOW_YES,   true  },

	// relations objects
	{ SHOW_GRANTS,        "grants",        6,  SHOW_NO,    false },
	{ SHOW_INDEXES,       "indexes",       7,  SHOW_YES,   false },

	// mcp
	{ SHOW_TOOLS,         "tools",         5,  SHOW_NO,    false },
	{ SHOW_RESOURCES,     "resources",     9,  SHOW_NO,    false },

	// config and state
	{ SHOW_STATE,         "state",         5,  SHOW_NO,    true  },
	{ SHOW_ALL,           "all",           3,  SHOW_NO,    true  },
	{ SHOW_CONFIG,        "config",        6,  SHOW_NO,    true  },
	{ 0,                   NULL,           0,  SHOW_NO,    false }
};

static inline ShowCmd*
show_cmd_find(Str* section)
{
	for (auto i = 0; show_cmds[i].section; i++)
	{
		auto cmd = &show_cmds[i];
		if (str_is_case(section, cmd->section, cmd->section_size))
			return cmd;
	}
	return NULL;
}

static ShowCmd*
fn_show_command(Call* self, Str* section, Str* name, Str* on, int* flags)
{
	// (section [, name, on, ...])
	str_init(section);
	str_init(name);
	str_init(on);

	// section
	auto argv = self->argv;
	call_arg(self, 0, TYPE_STRING);
	*section = argv[0].string;

	// [name]
	if (self->argc >= 2 && argv[1].type != TYPE_NULL)
	{
		call_arg(self, 1, TYPE_STRING);
		*name = argv[1].string;
	}

	// [on]
	if (self->argc >= 3 && argv[2].type != TYPE_NULL)
	{
		call_arg(self, 2, TYPE_STRING);
		*on = argv[2].string;
	}

	// read flags
	int mask = FMINIMAL;
	for (auto arg = 3; arg < self->argc; arg++)
	{
		// int (mask)
		if (argv[arg].type == TYPE_INT)
		{
			// rewrite flags
			mask = argv[arg].integer;
			continue;
		}

		// string
		call_arg(self, arg, TYPE_STRING);
		auto at = &argv[arg].string;
		if (str_is(at, "create", 6))
			mask |= FCREATE;
		else
		if (str_is(at, "verbose", 7))
			mask &= ~FMINIMAL;
		else
		if (str_is(at, "from", 4))
			mask |= FFROM;
		else
			call_error_noargs(self, "unknown flag");
	}
	*flags = mask;

	// match command
	auto cmd = show_cmd_find(section);
	if (! cmd)
		return NULL;

	// [name]
	switch (cmd->name) {
	case SHOW_NO:
		if (! str_empty(name))
			call_error_noargs(self, "unexpected 'name' argument");
		break;
	case SHOW_YES:
		if (str_empty(name))
			call_error_noargs(self, "'name' argument is missing for '{str}'", section);
		break;
	case SHOW_MAYBE:
		break;
	}

	return cmd;
}

static void
fn_show(Call* self)
{
	// (section, [name, on, ...])
	Str  section;
	Str  name;
	Str  on;
	int  flags;
	auto cmd = fn_show_command(self, &section, &name, &on, &flags);

	auto buf = buf_create();
	errdefer_buf(buf);

	// config or state option
	if (! cmd)
	{
		if (!str_empty(&name) || !str_empty(&on))
			call_error_noargs(self, "unexpected arguments");

		// config or state
		auto opt = opts_find(&config()->opts, &section);
		if (! opt)
			opt = opts_find(&state()->opts, &section);
		if (!opt || opt_is(opt, OPT_S))
			call_error_noargs(self, "option '{str}' is not found", &section);

		local_encode_opt(self->local, buf, opt);
		value_set_json_buf(self->result, buf);
		return;
	}

	// wrap in array for FROM SHOW
	auto wrap = false;
	if (cmd->obj && flags_has(flags, FFROM))
		wrap = true;
	if (wrap)
		encode_array(buf);

	auto db = catalog_find_db(&share()->storage->databases, &self->local->db, true);
	auto catalog = &db->catalog;
	switch (cmd->id) {
	case SHOW_REPLICAS:
	{
		replicas_list(&share()->repl->replicas, buf, NULL, flags);
		break;
	}
	case SHOW_REPLICA:
	{
		Uuid id;
		uuid_set(&id, &name);
		replicas_list(&share()->repl->replicas, buf, &id, flags);
		break;
	}
	case SHOW_REPL:
	{
		repl_status(share()->repl, buf);
		break;
	}
	case SHOW_WAL:
	{
		wal_status(&share()->storage->wal, buf);
		break;
	}
	case SHOW_METRICS:
	{
		rpc(&runtime()->task, MSG_SHOW_METRICS, &buf);
		break;
	}
	case SHOW_LOCKS:
	{
		locks_list(&runtime()->locks, buf);
		break;
	}
	case SHOW_LIMITS:
	{
		limits_write(&db->config->limits, buf);
		break;
	}
	case SHOW_API:
	{
		apis_write(&db->config->apis, buf, flags);
		break;
	}
	case SHOW_MEMORY:
	{
		usage_status(&db->memory, buf);
		break;
	}
	case SHOW_DATABASES:
	{
		rels_list(&share()->storage->databases.rels, REL_DB, buf, NULL, flags);
		break;
	}
	case SHOW_DATABASE:
	{
		rels_list(&share()->storage->databases.rels, REL_DB, buf, &name, flags);
		break;
	}
	case SHOW_TABLES:
	{
		rels_list(&catalog->rels, REL_TABLE, buf, NULL, flags);
		break;
	}
	case SHOW_TABLE:
	{
		rels_list(&catalog->rels, REL_TABLE, buf, &name, flags);
		break;
	}
	case SHOW_FUNCTIONS:
	{
		rels_list(&catalog->rels, REL_UDF, buf, NULL, flags);
		break;
	}
	case SHOW_FUNCTION:
	{
		rels_list(&catalog->rels, REL_UDF, buf, &name, flags);
		break;
	}
	case SHOW_RELS:
	{
		rels_list_rel(&catalog->rels, buf, NULL, flags);
		break;
	}
	case SHOW_REL:
	{
		rels_list_rel(&catalog->rels, buf, &name, flags);
		break;
	}
	case SHOW_GRANTS:
	{
		grants_write(&db->config->grants, buf, 0);
		break;
	}
	case SHOW_INDEXES:
	{
		auto table = catalog_find_table(catalog, &name, true);
		table_index_list(table, buf, NULL, flags);
		break;
	}
	case SHOW_TOOLS:
	{
		catalog_mcp_tools(catalog, buf);
		break;
	}
	case SHOW_RESOURCES:
	{
		catalog_mcp_resources(catalog, buf);
		break;
	}
	case SHOW_STATE:
	{
		storage_state(share()->storage, buf);
		break;
	}
	case SHOW_ALL:
	case SHOW_CONFIG:
	{
		encode_obj(buf);
		list_foreach(&config()->opts.list)
		{
			auto opt = list_at(Opt, link);
			if (opt_is(opt, OPT_H) || opt_is(opt, OPT_S))
				continue;
			encode_str(buf, &opt->name);
			local_encode_opt(self->local, buf, opt);
		}
		encode_obj_end(buf);
		break;
	}
	default:
		abort();
	}

	if (wrap)
		encode_array_end(buf);

	value_set_json_buf(self->result, buf);
}

void
fn_system_register(Functions* self)
{
	// show(section, name, on, ...)
	auto func = function_allocate(TYPE_JSON, "show", fn_show);
	function_unset(func, FN_CONST);
	functions_add(self, func);
}
