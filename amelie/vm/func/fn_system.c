
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>

static void
fn_config(Fn* self)
{
	fn_expect(self, 0);
	auto buf = opts_list(&config()->opts);
	value_set_json_buf(self->result, buf);
}

static void
fn_state(Fn* self)
{
	fn_expect(self, 0);
	auto buf = storage_state(share()->storage);
	value_set_json_buf(self->result, buf);
}

static void
fn_users(Fn* self)
{
	fn_expect(self, 0);
	auto buf = user_mgr_list(share()->user_mgr, NULL);
	value_set_json_buf(self->result, buf);
}

static void
fn_user(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	auto buf = user_mgr_list(share()->user_mgr, &self->argv[0].string);
	value_set_json_buf(self->result, buf);
}

static void
fn_replicas(Fn* self)
{
	fn_expect(self, 0);
	auto buf = replica_mgr_list(&share()->repl->replica_mgr, NULL);
	value_set_json_buf(self->result, buf);
}

static void
fn_replica(Fn* self)
{
	auto argv = self->argv;
	fn_expect(self, 1);
	if (argv[0].type == TYPE_STRING)
	{
		Uuid id;
		uuid_set(&id, &argv[0].string);
		auto buf = replica_mgr_list(&share()->repl->replica_mgr, &id);
		value_set_json_buf(self->result, buf);
	} else
	if (argv[0].type == TYPE_UUID)
	{
		auto buf = replica_mgr_list(&share()->repl->replica_mgr, &argv[0].uuid);
		value_set_json_buf(self->result, buf);
	} else {
		fn_unsupported(self, 0);
	}
}

static void
fn_repl(Fn* self)
{
	fn_expect(self, 0);
	auto buf = repl_status(share()->repl);
	value_set_json_buf(self->result, buf);
}

static void
fn_databases(Fn* self)
{
	fn_expect(self, 0);
	auto buf = db_mgr_list(&share()->storage->catalog.db_mgr, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_database(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	auto buf = db_mgr_list(&share()->storage->catalog.db_mgr, &self->argv[0].string, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_tables(Fn* self)
{
	fn_expect(self, 0);
	auto buf = table_mgr_list(&share()->storage->catalog.table_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_table(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str db;
	str_init(&db);
	if (str_split(&name, &db, '.'))
		str_advance(&name, str_size(&db) + 1);
	else
		str_set(&db, "main", 4);
	auto buf = table_mgr_list(&share()->storage->catalog.table_mgr, &db, &name, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_functions(Fn* self)
{
	fn_expect(self, 0);
	auto buf = udf_mgr_list(&share()->storage->catalog.udf_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_function(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str db;
	str_init(&db);
	if (str_split(&name, &db, '.'))
		str_advance(&name, str_size(&db) + 1);
	else
		str_set(&db, "main", 4);
	auto buf = udf_mgr_list(&share()->storage->catalog.udf_mgr, &db, &name, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_wal(Fn* self)
{
	fn_expect(self, 0);
	auto buf = wal_status(&share()->storage->wal_mgr.wal);
	value_set_json_buf(self->result, buf);
}

static void
fn_metrics(Fn* self)
{
	fn_expect(self, 0);
	Buf* buf;
	rpc(&runtime()->task, MSG_SHOW_METRICS, 1, &buf);
	value_set_json_buf(self->result, buf);
}

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
	// [section, name, db, extended]
	fn_expect(self, 4);
	fn_expect_arg(self, 0, TYPE_STRING);
	fn_expect_arg(self, 1, TYPE_STRING);
	fn_expect_arg(self, 2, TYPE_STRING);
	fn_expect_arg(self, 3, TYPE_BOOL);

	Str* section  = &self->argv[0].string;
	Str* name     = &self->argv[1].string;
	Str* db       = &self->argv[2].string;
	bool extended =  self->argv[3].integer;

	// match command
	Buf* buf = NULL;
	auto cmd = show_cmd_find(section);
	if (! cmd)
	{
		// config option
		if (str_empty(section))
			fn_error_noargs(self, "section name is not defined");
		if (! str_empty(name))
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
		if (str_empty(name))
			fn_error_noargs(self, "name is missing for '%.*s'",
			                str_size(section),
			                str_of(section));
	} else {
		if (! str_empty(name))
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
		Str* db_ref = NULL;
		if (! str_empty(db))
			db_ref = db;
		buf = table_mgr_list(&catalog->table_mgr, db_ref, NULL, extended);
		break;
	}
	case SHOW_TABLE:
	{
		Str* db_ref = NULL;
		if (! str_empty(db))
			db_ref = db;
		buf = table_mgr_list(&catalog->table_mgr, db_ref, name, extended);
		break;
	}
	case SHOW_FUNCTIONS:
	{
		Str* db_ref = NULL;
		if (! str_empty(db))
			db_ref = db;
		buf = udf_mgr_list(&catalog->udf_mgr, db_ref, NULL, extended);
		break;
	}
	case SHOW_FUNCTION:
	{
		Str* db_ref = NULL;
		if (! str_empty(db))
			db_ref = db;
		buf = udf_mgr_list(&catalog->udf_mgr, db_ref, name, extended);
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
	// config()
	Function* func;
	func = function_allocate(TYPE_JSON, "config", fn_config);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// state()
	func = function_allocate(TYPE_JSON, "state", fn_state);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// users()
	func = function_allocate(TYPE_JSON, "users", fn_users);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// user()
	func = function_allocate(TYPE_JSON, "user", fn_user);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// replicas()
	func = function_allocate(TYPE_JSON, "replicas", fn_replicas);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// replica()
	func = function_allocate(TYPE_JSON, "replica", fn_replica);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// replica(uuid)
	func = function_allocate(TYPE_JSON, "replica", fn_replica);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// repl()
	func = function_allocate(TYPE_JSON, "repl", fn_repl);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// replication()
	func = function_allocate(TYPE_JSON, "replication", fn_repl);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// databases()
	func = function_allocate(TYPE_JSON, "databases", fn_databases);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// database()
	func = function_allocate(TYPE_JSON, "database", fn_database);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// tables()
	func = function_allocate(TYPE_JSON, "tables", fn_tables);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// table()
	func = function_allocate(TYPE_JSON, "table", fn_table);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// functions()
	func = function_allocate(TYPE_JSON, "functions", fn_functions);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// function()
	func = function_allocate(TYPE_JSON, "function", fn_function);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// wal()
	func = function_allocate(TYPE_JSON, "wal", fn_wal);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// metrics()
	func = function_allocate(TYPE_JSON, "metrics", fn_metrics);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// show()
	func = function_allocate(TYPE_JSON, "show", fn_show);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);
}
