
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
#include <amelie_vm.h>

void
ccheckpoint(Vm* self, Op* op)
{
	// [workers]
	unused(self);
	int workers = op->a;
	rpc(&runtime()->task, MSG_CHECKPOINT, 1, workers);
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
	rpc(&runtime()->task, MSG_SYNC_USERS, 1, &share()->user_mgr->cache);
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
	rpc(&runtime()->task, MSG_SYNC_USERS, 1, &share()->user_mgr->cache);
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
	rpc(&runtime()->task, MSG_SYNC_USERS, 1, &share()->user_mgr->cache);
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
	control_save_state();
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
	control_save_state();
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
