
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
#include <amelie_db>
#include <amelie_sync>
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
	SHOW_STORAGES,
	SHOW_STORAGE,
	SHOW_DATABASES,
	SHOW_DATABASE,
	SHOW_TABLES,
	SHOW_TABLE,
	SHOW_INDEXES,
	SHOW_INDEX,
	SHOW_TIERS,
	SHOW_TIER,
	SHOW_PARTITIONS,
	SHOW_PARTITION,
	SHOW_FUNCTIONS,
	SHOW_FUNCTION,
	SHOW_STATE,
	SHOW_ALL,
	SHOW_CONFIG,
	SHOW_LOCKS
};

typedef struct ShowCmd ShowCmd;

struct ShowCmd
{
	int         id;
	const char* section;
	int         section_size;
	bool        has_name;
	bool        has_on;
};

static ShowCmd show_cmds[] =
{
	{ SHOW_USERS,      "users",       5,  false, false },
	{ SHOW_USER,       "user",        4,  true,  false },
	{ SHOW_REPLICAS,   "replicas",    8,  false, false },
	{ SHOW_REPLICA,    "replica",     7,  true,  false },
	{ SHOW_REPL,       "repl",        4,  false, false },
	{ SHOW_REPL,       "replication", 11, false, false },
	{ SHOW_WAL,        "wal",         3,  false, false },
	{ SHOW_METRICS,    "metrics",     7,  false, false },
	{ SHOW_STORAGES,   "storages",    8,  false, false },
	{ SHOW_STORAGE,    "storage",     7,  true,  false },
	{ SHOW_DATABASES,  "databases",   9,  false, false },
	{ SHOW_DATABASE,   "database",    8,  true,  false },
	{ SHOW_TABLES,     "tables",      6,  false, false },
	{ SHOW_TABLE,      "table",       5,  true,  false },
	{ SHOW_INDEXES,    "indexes",     7,  false, true  },
	{ SHOW_INDEX,      "index",       5,  true,  true  },
	{ SHOW_TIERS,      "tiers",       5,  false, true  },
	{ SHOW_TIER,       "tier",        4,  true,  true  },
	{ SHOW_PARTITIONS, "partitions",  10, false, true  },
	{ SHOW_PARTITION,  "partition",   9,  true,  true  },
	{ SHOW_FUNCTIONS,  "functions",   9,  false, false },
	{ SHOW_FUNCTION,   "function",    8,  true,  false },
	{ SHOW_STATE,      "state",       5,  false, false },
	{ SHOW_ALL,        "all",         3,  false, false },
	{ SHOW_CONFIG,     "config",      6,  false, false },
	{ SHOW_LOCKS,      "locks",       5,  false, false },
	{ 0,                NULL,         0,  false, false }
};

static inline ShowCmd*
show_cmd_find(Str* section)
{
	for (auto i = 0; show_cmds[i].section; i++)
	{
		auto cmd = &show_cmds[i];
		if (str_is(section, cmd->section, cmd->section_size))
			return cmd;
	}
	return NULL;
}

static void
fn_show(Fn* self)
{
	auto db = &self->local->db;

	// [section, name, on, extended]
	Str  section_none;
	Str* section  = NULL;
	Str* name     = NULL;
	Str* on       = NULL;
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
		extended =  self->argv[2].integer;
		break;
	case 4:
		// [section, name, on, bool]
		fn_expect_arg(self, 0, TYPE_STRING);
		fn_expect_arg(self, 1, TYPE_STRING);
		fn_expect_arg(self, 2, TYPE_STRING);
		fn_expect_arg(self, 3, TYPE_BOOL);
		section  = &self->argv[0].string;
		name     = &self->argv[1].string;
		on       = &self->argv[2].string;
		extended =  self->argv[3].integer;
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

	// validate arguments

	// name
	if (cmd->has_name) {
		if (!name || str_empty(name))
			fn_error_noargs(self, "name argument is missing for '%.*s'",
			                str_size(section),
			                str_of(section));
	} else {
		if (name && !str_empty(name))
			fn_error_noargs(self, "unexpected name argument");
	}

	// on
	if (cmd->has_on) {
		if (!on || str_empty(on))
			fn_error_noargs(self, "on argument is missing for '%.*s'",
			                str_size(section),
			                str_of(section));
	} else {
		if (on && !str_empty(on))
			fn_error_noargs(self, "unexpected on argument");
	}

	// prepare flags
	int flags = FMETRICS;
	if (! extended)
		flags |= FMINIMAL;

	auto catalog = &share()->db->catalog;
	switch (cmd->id) {
	case SHOW_USERS:
	{
		buf = user_mgr_list(share()->user_mgr, NULL, flags);
		break;
	}
	case SHOW_USER:
	{
		buf = user_mgr_list(share()->user_mgr, name, flags);
		break;
	}
	case SHOW_REPLICAS:
	{
		buf = replica_mgr_list(&share()->repl->replica_mgr, NULL, flags);
		break;
	}
	case SHOW_REPLICA:
	{
		Uuid id;
		uuid_set(&id, name);
		buf = replica_mgr_list(&share()->repl->replica_mgr, &id, flags);
		break;
	}
	case SHOW_REPL:
	{
		buf = repl_status(share()->repl);
		break;
	}
	case SHOW_WAL:
	{
		buf = wal_status(&share()->db->wal_mgr.wal);
		break;
	}
	case SHOW_METRICS:
	{
		rpc(&runtime()->task, MSG_SHOW_METRICS, &buf);
		break;
	}
	case SHOW_STORAGES:
	{
		buf = storage_mgr_list(&catalog->storage_mgr, NULL, flags);
		break;
	}
	case SHOW_STORAGE:
	{
		buf = storage_mgr_list(&catalog->storage_mgr, name, flags);
		break;
	}
	case SHOW_DATABASES:
	{
		buf = database_mgr_list(&catalog->db_mgr, NULL, flags);
		break;
	}
	case SHOW_DATABASE:
	{
		buf = database_mgr_list(&catalog->db_mgr, name, flags);
		break;
	}
	case SHOW_TABLES:
	{
		buf = table_mgr_list(&catalog->table_mgr, db, NULL, flags);
		break;
	}
	case SHOW_TABLE:
	{
		buf = table_mgr_list(&catalog->table_mgr, db, name, flags);
		break;
	}
	case SHOW_INDEXES:
	{
		auto table = table_mgr_find(&catalog->table_mgr, db, on, true);
		buf = table_index_list(table, NULL, flags);
		break;
	}
	case SHOW_INDEX:
	{
		auto table = table_mgr_find(&catalog->table_mgr, db, on, true);
		buf = table_index_list(table, name, flags);
		break;
	}
	case SHOW_TIERS:
	{
		auto table = table_mgr_find(&catalog->table_mgr, db, on, true);
		buf = table_tier_list(table, NULL, flags);
		break;
	}
	case SHOW_TIER:
	{
		auto table = table_mgr_find(&catalog->table_mgr, db, on, true);
		buf = table_tier_list(table, name, flags);
		break;
	}
	case SHOW_PARTITIONS:
	{
		auto table = table_mgr_find(&catalog->table_mgr, db, on, true);
		buf = part_mgr_status(&table->part_mgr, NULL, flags);
		break;
	}
	case SHOW_PARTITION:
	{
		auto table = table_mgr_find(&catalog->table_mgr, db, on, true);
		buf = part_mgr_status(&table->part_mgr, name, flags);
		break;
	}
	case SHOW_FUNCTIONS:
	{
		buf = udf_mgr_list(&catalog->udf_mgr, db, NULL, flags);
		break;
	}
	case SHOW_FUNCTION:
	{
		buf = udf_mgr_list(&catalog->udf_mgr, db, name, flags);
		break;
	}
	case SHOW_STATE:
	{
		buf = db_state(share()->db);
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
	case SHOW_LOCKS:
	{
		buf = buf_create();
		lock_mgr_list(&runtime()->lock_mgr, buf);
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
