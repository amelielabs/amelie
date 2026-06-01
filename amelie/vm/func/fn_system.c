
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
	SHOW_REPLICAS,
	SHOW_REPLICA,
	SHOW_REPL,
	SHOW_WAL,
	SHOW_METRICS,
	SHOW_GRANTS,
	SHOW_USERS,
	SHOW_USER,
	SHOW_TABLES,
	SHOW_TABLE,
	SHOW_INDEXES,
	SHOW_INDEX,
	SHOW_CLONES,
	SHOW_CLONE,
	SHOW_PARTITIONS,
	SHOW_PARTITION,
	SHOW_FUNCTIONS,
	SHOW_FUNCTION,
	SHOW_TOPICS,
	SHOW_TOPIC,
	SHOW_SUBSCRIPTIONS,
	SHOW_SUBSCRIPTION,
	SHOW_RELS,
	SHOW_REL,
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
	bool        obj;
};

static ShowCmd show_cmds[] =
{
	{ SHOW_REPLICAS,      "replicas",      8,  false, false, false },
	{ SHOW_REPLICA,       "replica",       7,  true,  false, true  },
	{ SHOW_REPL,          "repl",          4,  false, false, true  },
	{ SHOW_REPL,          "replication",   11, false, false, true  },
	{ SHOW_WAL,           "wal",           3,  false, false, true  },
	{ SHOW_METRICS,       "metrics",       7,  false, false, true  },
	{ SHOW_GRANTS,        "grants",        6,  false, true,  false },
	{ SHOW_USERS,         "users",         5,  false, false, false },
	{ SHOW_USER,          "user",          4,  true,  false, true  },
	{ SHOW_TABLES,        "tables",        6,  false, false, false },
	{ SHOW_TABLE,         "table",         5,  true,  false, true  },
	{ SHOW_INDEXES,       "indexes",       7,  false, true,  false },
	{ SHOW_INDEX,         "index",         5,  true,  true,  true  },
	{ SHOW_CLONES,        "clones",        6,  false, false, false },
	{ SHOW_CLONE,         "clone",         5,  true,  false, true  },
	{ SHOW_PARTITIONS,    "partitions",    10, false, true,  false },
	{ SHOW_PARTITION,     "partition",     9,  true,  true,  true  },
	{ SHOW_FUNCTIONS,     "functions",     9,  false, false, false },
	{ SHOW_FUNCTION,      "function",      8,  true,  false, true  },
	{ SHOW_TOPICS,        "topics",        6,  false, false, false },
	{ SHOW_TOPIC,         "topic",         5,  true,  false, true  },
	{ SHOW_SUBSCRIPTIONS, "subscriptions", 13, false, false, false },
	{ SHOW_SUBSCRIPTION,  "subscription",  12, true,  false, true  },
	{ SHOW_SUBSCRIPTIONS, "subs",          4,  false, false, false },
	{ SHOW_SUBSCRIPTION,  "sub",           3,  true,  false, true  },
	{ SHOW_RELS,          "rels",          4,  false, false, false },
	{ SHOW_REL,           "rel",           3,  true,  false, true  },
	{ SHOW_STATE,         "state",         5,  false, false, true  },
	{ SHOW_ALL,           "all",           3,  false, false, true  },
	{ SHOW_CONFIG,        "config",        6,  false, false, true  },
	{ SHOW_LOCKS,         "locks",         5,  false, false, false },
	{ 0,                   NULL,           0,  false, false, false }
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
	// [section, name, on, verbose]
	Str  section_none;
	Str* section  = NULL;
	Str* name     = NULL;
	Str* on       = NULL;
	bool verbose  = false;

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
			verbose = self->argv[1].integer;
		else
			fn_error_arg(self, 1, "string or bool expected");
		break;
	case 3:
		// [section, name, bool]
		fn_expect_arg(self, 0, TYPE_STRING);
		fn_expect_arg(self, 1, TYPE_STRING);
		fn_expect_arg(self, 2, TYPE_BOOL);
		section = &self->argv[0].string;
		name    = &self->argv[1].string;
		verbose =  self->argv[2].integer;
		break;
	case 4:
		// [section, name, on, bool]
		fn_expect_arg(self, 0, TYPE_STRING);
		fn_expect_arg(self, 1, TYPE_STRING);
		fn_expect_arg(self, 2, TYPE_STRING);
		fn_expect_arg(self, 3, TYPE_BOOL);
		section = &self->argv[0].string;
		name    = &self->argv[1].string;
		on      = &self->argv[2].string;
		verbose =  self->argv[3].integer;
		break;
	default:
		fn_error_noargs(self, "invalid number of arguments");
		break;
	}
	str_set(&section_none, "all", 3);

	// match command
	auto buf = buf_create();
	errdefer_buf(buf);

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
			fn_error_noargs(self, "option '{str}' is not found", section);
		local_encode_opt(self->local, buf, opt);
		value_set_json_buf(self->result, buf);
		return;
	}

	// validate arguments

	// name
	if (cmd->has_name) {
		if (!name || str_empty(name))
			fn_error_noargs(self, "name argument is missing for '{str}'",
			                section);
	} else {
		if (name && !str_empty(name))
			fn_error_noargs(self, "unexpected name argument");
	}

	// on
	if (cmd->has_on) {
		if (!on || str_empty(on))
			fn_error_noargs(self, "on argument is missing for '{str}'",
			                section);
	} else {
		if (on && !str_empty(on))
			fn_error_noargs(self, "unexpected on argument");
	}


	// cover in [] if run as show_from()
	auto wrap = false;
	auto show_from =
		str_is(&self->function->name, "show_from", 9) ||
		str_is(&self->function->name, "show_from_all", 13);
	if (cmd->obj && show_from)
		wrap = true;
	if (wrap)
		encode_array(buf);

	// prepare flags
	int flags = FMETRICS;
	if (! verbose)
		flags |= FMINIMAL;

	// [all]
	auto user = &self->local->user;
	auto all  =
		str_is(&self->function->name, "show_all", 8) ||
		str_is(&self->function->name, "show_from_all", 13);

	auto catalog = &share()->db->catalog;
	switch (cmd->id) {
	case SHOW_REPLICAS:
	{
		replica_mgr_list(&share()->repl->replica_mgr, buf, NULL, flags);
		break;
	}
	case SHOW_REPLICA:
	{
		Uuid id;
		uuid_set(&id, name);
		replica_mgr_list(&share()->repl->replica_mgr, buf, &id, flags);
		break;
	}
	case SHOW_REPL:
	{
		repl_status(share()->repl, buf);
		break;
	}
	case SHOW_WAL:
	{
		wal_status(&share()->db->wal, buf);
		break;
	}
	case SHOW_METRICS:
	{
		rpc(&runtime()->task, MSG_SHOW_METRICS, &buf);
		break;
	}
	case SHOW_GRANTS:
	{
		auto rel = catalog_find(catalog, REL_UNDEF, user, on, true);
		if (rel->grants)
			grants_write(rel->grants, buf, 0);
		else
			encode_null(buf);
		break;
	}
	case SHOW_USERS:
	{
		// created users
		rel_mgr_list(&catalog->users, REL_USER, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_USER:
	{
		rel_mgr_list(&catalog->users, REL_USER, buf, NULL, name, all, flags);
		break;
	}
	case SHOW_TABLES:
	{
		rel_mgr_list(&catalog->rels, REL_TABLE, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_TABLE:
	{
		rel_mgr_list(&catalog->rels, REL_TABLE, buf, user, name, all, flags);
		break;
	}
	case SHOW_INDEXES:
	{
		// todo: only owned
		auto table = catalog_find_table(catalog, user, on, true);
		table_index_list(table, buf, NULL, flags);
		break;
	}
	case SHOW_INDEX:
	{
		auto table = catalog_find_table(catalog, user, on, true);
		table_index_list(table, buf, name, flags);
		break;
	}
	case SHOW_CLONES:
	{
		rel_mgr_list(&catalog->rels, REL_CLONE, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_CLONE:
	{
		rel_mgr_list(&catalog->rels, REL_CLONE, buf, user, name, all, flags);
		break;
	}
	case SHOW_PARTITIONS:
	{
		// todo: only owned
		auto table = catalog_find_table(catalog, user, on, true);
		part_mgr_list(&table->part_mgr, buf, NULL, flags);
		break;
	}
	case SHOW_PARTITION:
	{
		auto table = catalog_find_table(catalog, user, on, true);
		part_mgr_list(&table->part_mgr, buf, name, flags);
		break;
	}
	case SHOW_FUNCTIONS:
	{
		rel_mgr_list(&catalog->rels, REL_UDF, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_FUNCTION:
	{
		rel_mgr_list(&catalog->rels, REL_UDF, buf, user, name, all, flags);
		break;
	}
	case SHOW_TOPICS:
	{
		rel_mgr_list(&catalog->rels, REL_TOPIC, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_TOPIC:
	{
		rel_mgr_list(&catalog->rels, REL_TOPIC, buf, user, name, all, flags);
		break;
	}
	case SHOW_SUBSCRIPTIONS:
	{
		rel_mgr_list(&catalog->rels, REL_SUBSCRIPTION, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_SUBSCRIPTION:
	{
		rel_mgr_list(&catalog->rels, REL_SUBSCRIPTION, buf, user, name, all, flags);
		break;
	}
	case SHOW_RELS:
	{
		rel_mgr_list_rel(&catalog->rels, buf, user, NULL, all, flags);
		break;
	}
	case SHOW_REL:
	{
		rel_mgr_list_rel(&catalog->rels, buf, user, name, all, flags);
		break;
	}
	case SHOW_STATE:
	{
		db_state(share()->db, buf);
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
	case SHOW_LOCKS:
	{
		lock_mgr_list(&runtime()->lock_mgr, buf);
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
fn_system_register(FunctionMgr* self)
{
	// show()
	auto func = function_allocate(TYPE_JSON, "show", fn_show);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// show_all()
	func = function_allocate(TYPE_JSON, "show_all", fn_show);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// show_from()
	func = function_allocate(TYPE_JSON, "show_from", fn_show);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);

	// show_from_all()
	func = function_allocate(TYPE_JSON, "show_from_all", fn_show);
	function_unset(func, FN_CONST);
	function_mgr_add(self, func);
}
