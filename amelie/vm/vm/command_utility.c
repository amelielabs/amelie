
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

#include <amelie_runtime.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
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
#include <amelie_vm.h>

void
cshow(Vm* self, Op* op)
{
	// [result, options_offset]
	auto pos = code_data_at(self->code_data, op->b);
	Str  section;
	Str  name;
	Str  schema;
	bool extended;
	json_read_string(&pos, &section);
	json_read_string(&pos, &name);
	json_read_string(&pos, &schema);
	json_read_bool(&pos, &extended);

	Buf* buf = NULL;
	auto catalog = &share()->db->catalog;
	if (str_is(&section, "users", 5))
	{
		buf = user_mgr_list(share()->user_mgr, NULL);
	} else
	if (str_is(&section, "user", 4))
	{
		buf = user_mgr_list(share()->user_mgr, &name);
	} else
	if (str_is(&section, "replicas", 8))
	{
		buf = replica_mgr_list(&share()->repl->replica_mgr, NULL);
	} else
	if (str_is(&section, "replica", 7))
	{
		Uuid id;
		uuid_set(&id, &name);
		buf = replica_mgr_list(&share()->repl->replica_mgr, &id);
	} else
	if (str_is(&section, "repl", 4) ||
	    str_is(&section, "replication", 11))
	{
		buf = repl_status(share()->repl);
	} else
	if (str_is(&section, "wal", 3))
	{
		buf = wal_status(&share()->db->wal_mgr.wal);
	} else
	if (str_is(&section, "metrics", 7))
	{
		rpc(global()->control->system, RPC_SHOW_METRICS, 1, &buf);
	} else
	if (str_is(&section, "schemas", 7))
	{
		buf = schema_mgr_list(&catalog->schema_mgr, NULL, extended);
	} else
	if (str_is(&section, "schema", 6))
	{
		buf = schema_mgr_list(&catalog->schema_mgr, &name, extended);
	} else
	if (str_is(&section, "tables", 6))
	{
		Str* schema_ref = NULL;
		if (! str_empty(&schema))
			schema_ref = &schema;
		buf = table_mgr_list(&catalog->table_mgr, schema_ref, NULL, extended);
	} else
	if (str_is(&section, "table", 5))
	{
		Str* schema_ref = NULL;
		if (! str_empty(&schema))
			schema_ref = &schema;
		buf = table_mgr_list(&catalog->table_mgr, schema_ref, &name, extended);
	} else
	if (str_is(&section, "state", 5))
	{
		buf = db_state(share()->db);
	} else
	if (str_is(&section, "config", 6) ||
	    str_is(&section, "all", 3))
	{
		auto opt = opts_find(&global()->config->opts, &name);
		if (opt && opt_is(opt, OPT_S))
			opt = NULL;
		if (unlikely(opt == NULL))
			error("option '%.*s' not found", str_size(&name), str_of(&name));
		buf = buf_create();
		opt_encode(opt, buf);
	} else
	{
		// config
		auto opt = opts_find(&global()->config->opts, &section);
		if (opt && opt_is(opt, OPT_S))
			opt = NULL;
		if (unlikely(opt == NULL))
			error("option '%.*s' not found", str_size(&section), str_of(&section));
		buf = buf_create();
		opt_encode(opt, buf);
	}

	value_set_json_buf(reg_at(&self->r, op->a), buf);
}

void
ccheckpoint(Vm* self, Op* op)
{
	// [workers]
	unused(self);
	int workers = op->a;
	rpc(global()->control->system, RPC_CHECKPOINT, 1, workers);
}

void
cuser_create_token(Vm* self, Op* op)
{
	// [result, options_offset]
	auto pos = code_data_at(self->code_data, op->b);
	Str  name;
	Str  interval;
	json_read_string(&pos, &name);
	json_read_string(&pos, &interval);

	// find user
	auto user_mgr = share()->user_mgr;
	auto user = user_cache_find(&user_mgr->cache, &name);
	if (! user)
		error("user '%.*s' not found", str_size(&name), str_of(&name));

	// ensure user has a secret
	if (str_empty(&user->config->secret))
		error("user '%.*s' has no secret", str_size(&name), str_of(&name));

	// set expire interval
	Interval iv;
	interval_init(&iv);
	interval_set(&iv, &interval);

	Timestamp expire;
	timestamp_init(&expire);
	timestamp_set_unixtime(&expire, time_us());
	timestamp_add(&expire, &iv);

	// generate token
	auto jwt = jwt_create(&user->config->name, &user->config->secret, &expire);
	defer_buf(jwt);
	auto buf = buf_create();
	encode_buf(buf, jwt);

	value_set_json_buf(reg_at(&self->r, op->a), buf);
}

void
cuser_create(Vm* self, Op* op)
{
	// [config_offset, if_not_exists]
	auto pos = code_data_at(self->code_data, op->a);
	auto config = user_config_read(&pos);
	defer(user_config_free, config);

	bool if_not_exists = op->b;
	user_mgr_create(share()->user_mgr, config, if_not_exists);

	// sync user caches among frontends
	rpc(global()->control->system, RPC_SYNC_USERS, 0);
}

void
cuser_drop(Vm* self, Op* op)
{
	// [name, if_exists]
	auto pos = code_data_at(self->code_data, op->a);
	Str name;
	json_read_string(&pos, &name);

	bool if_exists = op->b;
	user_mgr_drop(share()->user_mgr, &name, if_exists);

	// sync user caches among frontends
	rpc(global()->control->system, RPC_SYNC_USERS, 0);
}

void
cuser_alter(Vm* self, Op* op)
{
	// [config_offset]
	auto pos = code_data_at(self->code_data, op->a);
	auto config = user_config_read(&pos);
	defer(user_config_free, config);
	user_mgr_alter(share()->user_mgr, config);

	// sync user caches among frontends
	rpc(global()->control->system, RPC_SYNC_USERS, 0);
}

void
creplica_create(Vm* self, Op* op)
{
	// [config_offset, if_not_exists]
	auto pos = code_data_at(self->code_data, op->a);
	auto config = replica_config_read(&pos);
	defer(replica_config_free, config);

	bool if_not_exists = op->b;
	replica_mgr_create(&share()->repl->replica_mgr, config, if_not_exists);
}

void
creplica_drop(Vm* self, Op* op)
{
	// [id, if_exists]
	auto pos = code_data_at(self->code_data, op->a);
	Str  id_str;
	Uuid id;
	json_read_string(&pos, &id_str);
	uuid_set(&id, &id_str);

	bool if_exists = op->b;
	replica_mgr_drop(&share()->repl->replica_mgr, &id, if_exists);
}

void
crepl_start(Vm* self, Op* op)
{
	unused(self);
	unused(op);
	repl_start(share()->repl);
	control_save_state();
}

void
crepl_stop(Vm* self, Op* op)
{
	unused(self);
	unused(op);
	repl_stop(share()->repl);
	control_save_state();
}

void
crepl_subscribe(Vm* self, Op* op)
{
	// [id, if_exists]
	auto pos = code_data_at(self->code_data, op->a);
	Str  id;
	json_read_string(&pos, &id);
	repl_subscribe(share()->repl, &id);
	control_save_state();
}

void
crepl_unsubscribe(Vm* self, Op* op)
{
	unused(self);
	unused(op);
	repl_subscribe(share()->repl, NULL);
	control_save_state();
}

void
cddl(Vm* self, Op* op)
{
	// [op, flags]
	unused(self);
	auto pos = code_data_at(self->code_data, op->a);
	auto flags = op->b;
	catalog_execute(&share()->db->catalog, self->tr, pos, flags);
}
