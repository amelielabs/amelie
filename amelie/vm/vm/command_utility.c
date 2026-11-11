
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
#include <amelie_vm.h>

void
csystem_set_secret(Vm* self, Op* op)
{
	// [secret]
	auto pos = code_data_at(&self->program->code_data, op->a);
	Str secret;
	unpack_str(&pos, &secret);

	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	// create new system secret
	opt_string_set(&state()->secret, &secret);
	opt_int_set_next(&state()->secret_seq);

	control_state_write();
}

void
ccreate_token(Vm* self, Op* op)
{
	// PERM_CREATE_TOKEN
	check_permission(&self->gtr->tr, PERM_CREATE_TOKEN);

	// [result, options_offset]
	auto pos = code_data_at(&self->program->code_data, op->b);
	Str  name;
	Str  interval;
	unpack_str(&pos, &name);
	unpack_str(&pos, &interval);

	// find db
	auto db = catalog_find_db(&share()->storage->databases, &name, true);

	// allow to create tokens only for child dbs (or anyone if system)
	db_check_ownership(&self->gtr->tr, db);

	// set expire interval
	Interval iv;
	interval_init(&iv);
	interval_set(&iv, &interval);

	Timestamp expire;
	timestamp_init(&expire);
	timestamp_set_unixtime(&expire, time_us());
	timestamp_add(&expire, &iv);

	// generate token
	auto secret = opt_string_of(&state()->secret);
	auto jwt = jwt_create(&db->config->name, secret, &expire);
	defer_buf(jwt);
	auto buf = buf_create();
	encode_buf(buf, jwt);

	value_set_json_buf(reg_at(&self->r, op->a), buf);
}

void
ccheckpoint(Vm* self, Op* op)
{
	// [db*]

	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	auto db = (Db*)op->a;
	storage_checkpoint(share()->storage, db);
}

void
cbackup(Vm* self, Op* op)
{
	// [uuid_offset]
	auto id = (Uuid*)code_data_at(&self->program->code_data, op->a);

	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	backup(share()->storage, id);
}

void
creplica_create(Vm* self, Op* op)
{
	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	// [config_offset, if_not_exists]
	auto pos = code_data_at(&self->program->code_data, op->a);
	auto config = replica_config_read(&pos);
	defer(replica_config_free, config);

	bool if_not_exists = op->b;
	replicas_create(&share()->repl->replicas, config, if_not_exists);
	control_state_write();
}

void
creplica_drop(Vm* self, Op* op)
{
	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	// [id, if_exists]
	auto pos = code_data_at(&self->program->code_data, op->a);
	Str  id_str;
	Uuid id;
	unpack_str(&pos, &id_str);
	uuid_set(&id, &id_str);

	bool if_exists = op->b;
	replicas_drop(&share()->repl->replicas, &id, if_exists);
	control_state_write();
}

void
crepl_follow(Vm* self, Op* op)
{
	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	// [id, if_exists]
	auto pos = code_data_at(&self->program->code_data, op->a);
	Str  id;
	unpack_str(&pos, &id);
	repl_follow(share()->repl, &id);
	control_state_write();
}

void
crepl_unfollow(Vm* self, Op* op)
{
	// PERM_SYSTEM
	check_permission(&self->gtr->tr, PERM_SYSTEM);

	unused(self);
	unused(op);
	repl_follow(share()->repl, NULL);
	control_state_write();
}

void
cddl(Vm* self, Op* op)
{
	// [op, flags, dbs]
	unused(self);
	auto pos = code_data_at(&self->program->code_data, op->a);

	auto db = db_of(self->gtr->tr.db);
	auto flags = op->b;

	// use db catalog or storage catalog
	Catalog* catalog = &db->catalog;
	if  (op->c)
		catalog = &share()->storage->databases;
	catalog_execute(catalog, &self->gtr->tr, pos, flags);
}
