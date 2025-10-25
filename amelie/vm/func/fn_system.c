
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
#include <amelie_db.h>
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
	auto buf = db_state(share()->db);
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
fn_schemas(Fn* self)
{
	fn_expect(self, 0);
	auto buf = schema_mgr_list(&share()->db->catalog.schema_mgr, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_schema(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	auto buf = schema_mgr_list(&share()->db->catalog.schema_mgr, &self->argv[0].string, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_tables(Fn* self)
{
	fn_expect(self, 0);
	auto buf = table_mgr_list(&share()->db->catalog.table_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_table(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str schema;
	str_init(&schema);
	if (str_split(&name, &schema, '.'))
		str_advance(&name, str_size(&schema) + 1);
	else
		str_set(&schema, "public", 6);
	auto buf = table_mgr_list(&share()->db->catalog.table_mgr, &schema, &name, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_functions(Fn* self)
{
	fn_expect(self, 0);
	auto buf = udf_mgr_list(&share()->db->catalog.udf_mgr, NULL, NULL, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_function(Fn* self)
{
	fn_expect(self, 1);
	fn_expect_arg(self, 0, TYPE_STRING);
	Str name = self->argv[0].string;
	Str schema;
	str_init(&schema);
	if (str_split(&name, &schema, '.'))
		str_advance(&name, str_size(&schema) + 1);
	else
		str_set(&schema, "public", 6);
	auto buf = udf_mgr_list(&share()->db->catalog.udf_mgr, &schema, &name, true);
	value_set_json_buf(self->result, buf);
}

static void
fn_wal(Fn* self)
{
	fn_expect(self, 0);
	auto buf = wal_status(&share()->db->wal_mgr.wal);
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
	SHOW_SCHEMAS,
	SHOW_SCHEMA,
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
	{ SHOW_SCHEMAS,   "schemas",     7,  false  },
	{ SHOW_SCHEMA,    "schema",      6,  true   },
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
	// [section, name, schema, extended]
	fn_expect(self, 4);
	fn_expect_arg(self, 0, TYPE_STRING);
	fn_expect_arg(self, 1, TYPE_STRING);
	fn_expect_arg(self, 2, TYPE_STRING);
	fn_expect_arg(self, 3, TYPE_BOOL);

	Str* section  = &self->argv[0].string;
	Str* name     = &self->argv[1].string;
	Str* schema   = &self->argv[2].string;
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
		opt_encode(opt, buf);
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

	auto catalog = &share()->db->catalog;
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
		buf = wal_status(&share()->db->wal_mgr.wal);
		break;
	}
	case SHOW_METRICS:
	{
		rpc(&runtime()->task, MSG_SHOW_METRICS, 1, &buf);
		break;
	}
	case SHOW_SCHEMAS:
	{
		buf = schema_mgr_list(&catalog->schema_mgr, NULL, extended);
		break;
	}
	case SHOW_SCHEMA:
	{
		buf = schema_mgr_list(&catalog->schema_mgr, name, extended);
		break;
	}
	case SHOW_TABLES:
	{
		Str* schema_ref = NULL;
		if (! str_empty(schema))
			schema_ref = schema;
		buf = table_mgr_list(&catalog->table_mgr, schema_ref, NULL, extended);
		break;
	}
	case SHOW_TABLE:
	{
		Str* schema_ref = NULL;
		if (! str_empty(schema))
			schema_ref = schema;
		buf = table_mgr_list(&catalog->table_mgr, schema_ref, name, extended);
		break;
	}
	case SHOW_FUNCTIONS:
	{
		Str* schema_ref = NULL;
		if (! str_empty(schema))
			schema_ref = schema;
		buf = udf_mgr_list(&catalog->udf_mgr, schema_ref, NULL, extended);
		break;
	}
	case SHOW_FUNCTION:
	{
		Str* schema_ref = NULL;
		if (! str_empty(schema))
			schema_ref = schema;
		buf = udf_mgr_list(&catalog->udf_mgr, schema_ref, name, extended);
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
		buf = opts_list(&config()->opts);
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
	// system.config()
	Function* func;
	func = function_allocate(TYPE_JSON, "system", "config", fn_config);
	function_mgr_add(self, func);

	// system.state()
	func = function_allocate(TYPE_JSON, "system", "state", fn_state);
	function_mgr_add(self, func);

	// system.users()
	func = function_allocate(TYPE_JSON, "system", "users", fn_users);
	function_mgr_add(self, func);

	// system.user()
	func = function_allocate(TYPE_JSON, "system", "user", fn_user);
	function_mgr_add(self, func);

	// system.replicas()
	func = function_allocate(TYPE_JSON, "system", "replicas", fn_replicas);
	function_mgr_add(self, func);

	// system.replica()
	func = function_allocate(TYPE_JSON, "system", "replica", fn_replica);
	function_mgr_add(self, func);

	// system.replica(uuid)
	func = function_allocate(TYPE_JSON, "system", "replica", fn_replica);
	function_mgr_add(self, func);

	// system.repl()
	func = function_allocate(TYPE_JSON, "system", "repl", fn_repl);
	function_mgr_add(self, func);

	// system.replication()
	func = function_allocate(TYPE_JSON, "system", "replication", fn_repl);
	function_mgr_add(self, func);

	// system.schemas()
	func = function_allocate(TYPE_JSON, "system", "schemas", fn_schemas);
	function_mgr_add(self, func);

	// system.schema()
	func = function_allocate(TYPE_JSON, "system", "schema", fn_schema);
	function_mgr_add(self, func);

	// system.tables()
	func = function_allocate(TYPE_JSON, "system", "tables", fn_tables);
	function_mgr_add(self, func);

	// system.table()
	func = function_allocate(TYPE_JSON, "system", "table", fn_table);
	function_mgr_add(self, func);

	// system.functions()
	func = function_allocate(TYPE_JSON, "system", "functions", fn_functions);
	function_mgr_add(self, func);

	// system.function()
	func = function_allocate(TYPE_JSON, "system", "function", fn_function);
	function_mgr_add(self, func);

	// system.wal()
	func = function_allocate(TYPE_JSON, "system", "wal", fn_wal);
	function_mgr_add(self, func);

	// system.metrics()
	func = function_allocate(TYPE_JSON, "system", "metrics", fn_metrics);
	function_mgr_add(self, func);

	// system.show()
	func = function_allocate(TYPE_JSON, "system", "show", fn_show);
	function_mgr_add(self, func);
}
