
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
#include <amelie_vm.h>

void
ccheckpoint(Vm* self, Op* op)
{
	unused(self);
	unused(op);
	db_checkpoint(share()->db);
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
	rpc(&runtime()->task, MSG_SYNC_USERS, &share()->user_mgr->cache);
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
	rpc(&runtime()->task, MSG_SYNC_USERS, &share()->user_mgr->cache);
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
	rpc(&runtime()->task, MSG_SYNC_USERS, &share()->user_mgr->cache);
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

void
cddl_create_index(Vm* self, Op* op)
{
	// [op, flags]
	unused(self);
	auto pos = code_data_at(self->code_data, op->a);
	auto flags = op->b;
	db_create_index(share()->db, self->tr, pos, flags);
}

void
clock_rel(Vm* self, Op* op)
{
	// [name, name_rel, name_lock, if_not_exists]
	Str name;
	Str name_rel;
	Str name_lock;
	code_data_at_string(self->code_data, op->a, &name);
	code_data_at_string(self->code_data, op->b, &name_rel);
	code_data_at_string(self->code_data, op->c, &name_lock);

	// validate lock type
	auto lock_id = lock_id_of(&name_lock);
	if (lock_id == LOCK_NONE)
		error("lock: unrecognized lock type '%.*s'", str_size(&name_lock),
		      str_of(&name_lock));

	// ensure lock does not exists
	auto lock_mgr = &runtime()->lock_mgr;
	auto lock = lock_mgr_find(lock_mgr, &name);
	if (lock)
	{
		if (! op->d)
			error("lock: '%.*s' already exists", str_size(&name),
			      str_of(&name));
		return;
	}

	Relation* rel;

	// find breakpoint
	auto ref = lockable_mgr_find(&runtime()->lockable_mgr, &name_rel);
	if (ref)
	{
		// ensure relation is a breakpoint
		if (! ref->bp)
			error("lock: relation '%.*s' cannot be locked this way",
			      str_size(&name), str_of(&name));
		rel = &ref->rel;

		// enable breakpoint
		lockable_breakpoint_ref(ref);
	} else
	{
		// find table
		auto table = table_mgr_find(&share()->db->catalog.table_mgr,
		                            &self->local->db,
		                            &name_rel, true);
		rel = &table->rel;
	}

	// create detached lock
	lock_mgr_lock(&runtime()->lock_mgr, rel, lock_id,
	              &name,
	              source_function,
	              source_line);
}

void
cunlock_rel(Vm* self, Op* op)
{
	// [name, if_exists]
	Str name;
	code_data_at_string(self->code_data, op->a, &name);

	// ensure lock exists
	auto lock_mgr = &runtime()->lock_mgr;
	auto lock = lock_mgr_find(lock_mgr, &name);
	if (! lock)
	{
		if (! op->b)
			error("lock: '%.*s' not exists", str_size(&name),
			      str_of(&name));
		return;
	}

	// derefence breakpoint (disable on zero)
	if (lock->rel->lock_order < REL_MAX)
	{
		auto ref = &runtime()->lockable_mgr.list[lock->rel->lock_order];
		lockable_breakpoint_unref(ref);
	}

	unlock(lock);
}
