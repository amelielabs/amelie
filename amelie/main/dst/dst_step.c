
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

#include <amelie>
#include <amelie_main.h>
#include <amelie_main_dst.h>

static void
dst_stmt(DstUser* self)
{
	auto op = dst_log_add(&self->log);

	// generate relation (table, table_vector, clone, topic)
	DstRel* rel;
	for (;;)
	{
		auto rel_order = random_generate(&am_task->random) % self->rels_count;
		rel = dst_user_rel(self, rel_order);
		assert(rel);
		if (rel->type != DST_REL_SUBSCRIPTION)
			break;
	}
	op->rel = rel;

	// generate key
	uint64_t key_id = random_generate(&am_task->random) % opt_int_of(&self->dst->opt_keys);

	// PUBLISH
	if (rel->type == DST_REL_TOPIC)
	{
		op->op = DST_OP_PUBLISH;

		DstKey key;
		memset(&key, 0, sizeof(key));
		key.key   = key_id;
		key.value = random_generate(&am_task->random);

		buf_format(&self->log.sql,
		           "PUBLISH INTO topic_{u64} [{u64}, {i64}];",
		           rel->id, key.key, key.value);

		// add cdc event
		dst_rel_cdc(rel, &key);
		return;
	}

	// INSERT
	auto key = dst_rel_get(rel, key_id);
	if (! key)
	{
		op->op = DST_OP_INSERT;

		key = dst_key_allocate(key_id);
		dst_rel_set(rel, key);
		op->key = *key;

		if (rel->type == DST_REL_TABLE)
		{
			key->value = random_generate(&am_task->random);
			buf_format(&self->log.sql,
			           "INSERT INTO table_{u64} VALUES ({u64}, {i64});",
			           rel->id, key->key, key->value);
		} else
		if (rel->type == DST_REL_TABLE_VECTOR)
		{
			// table_vector
			key->value_vector[0] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[1] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[2] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[3] = random_generate_fp(&am_task->random, -1.0, 1.0);
			buf_format(&self->log.sql,
			           "INSERT INTO table_vector_{u64} VALUES ({u64}, vector [{f}, {f}, {f}, {f}]);",
			           rel->id,
			           key->key,
			           key->value_vector[0],
			           key->value_vector[1],
			           key->value_vector[2],
			           key->value_vector[3]);
		} else
		if (rel->type == DST_REL_CLONE)
		{
			key->value = random_generate(&am_task->random);
			buf_format(&self->log.sql,
			           "INSERT INTO clone_{u64}_{u64} VALUES ({u64}, {i64});",
			           rel->parent->id, rel->id, key->key, key->value);
		} else {
			abort();
		}
		dst_rel_cdc(rel, key);
		return;
	}
	op->prev = *key;

	// UPSERT (40%)
	uint64_t probability = random_generate(&am_task->random) % 100;
	if (probability < 40)
	{
		op->op = DST_OP_UPSERT;
		if (rel->type == DST_REL_TABLE)
		{
			key->value = random_generate(&am_task->random);
			buf_format(&self->log.sql,
			           "INSERT INTO table_{u64} VALUES ({u64}, null) ON CONFLICT DO UPDATE SET state = {i64};",
			           rel->id, key->key, key->value);
		} else
		if (rel->type == DST_REL_TABLE_VECTOR)
		{
			key->value_vector[0] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[1] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[2] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[3] = random_generate_fp(&am_task->random, -1.0, 1.0);
			buf_format(&self->log.sql,
			           "INSERT INTO table_vector_{u64} VALUES ({u64}, null) "
			           "ON CONFLICT DO UPDATE SET state = vector [{f}, {f}, {f}, {f}];",
			           rel->id,
			           key->key,
			           key->value_vector[0],
			           key->value_vector[1],
			           key->value_vector[2],
			           key->value_vector[3]);
		} else
		if (rel->type == DST_REL_CLONE)
		{
			key->value = random_generate(&am_task->random);
			buf_format(&self->log.sql,
			           "INSERT INTO clone_{u64}_{u64} VALUES ({u64}, null) ON CONFLICT DO UPDATE SET state = {i64};",
			           rel->parent->id, rel->id, key->key, key->value);
		} else {
			abort();
		}
		dst_rel_cdc(rel, key);

		op->key = *key;
		return;
	}

	// UPDATE (40%)
	if (probability < 80)
	{
		op->op = DST_OP_UPDATE;
		if (rel->type == DST_REL_TABLE)
		{
			key->value = random_generate(&am_task->random);
			buf_format(&self->log.sql,
			           "UPDATE table_{u64} SET state = {i64} WHERE id = {i64};",
			           rel->id, key->value, key->key);
		} else
		if (rel->type == DST_REL_TABLE_VECTOR)
		{
			key->value_vector[0] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[1] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[2] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[3] = random_generate_fp(&am_task->random, -1.0, 1.0);
			buf_format(&self->log.sql,
			           "UPDATE table_vector_{u64} "
			           "SET state = vector [{f}, {f}, {f}, {f}] WHERE id = {u64};",
			           rel->id,
			           key->value_vector[0],
			           key->value_vector[1],
			           key->value_vector[2],
			           key->value_vector[3],
			           key->key);
		} else
		if (rel->type == DST_REL_CLONE)
		{
			key->value = random_generate(&am_task->random);
			buf_format(&self->log.sql,
			           "UPDATE clone_{u64}_{u64} SET state = {i64} WHERE id = {i64};",
			           rel->parent->id, rel->id, key->value, key->key);
		} else {
			abort();
		}
		dst_rel_cdc(rel, key);
		op->key = *key;
		return;
	}

	// DELETE (20%)
	op->op = DST_OP_DELETE;
	if (rel->type == DST_REL_TABLE)
	{
		buf_format(&self->log.sql,
		           "DELETE FROM table_{u64} WHERE id = {u64};",
		            rel->id, key->key);
	} else
	if (rel->type == DST_REL_TABLE_VECTOR)
	{
		buf_format(&self->log.sql,
		           "DELETE FROM table_vector_{u64} WHERE id = {u64};",
		           rel->id, key->key);
	} else
	if (rel->type == DST_REL_CLONE)
	{
		buf_format(&self->log.sql,
		           "DELETE FROM clone_{u64}_{u64} WHERE id = {u64};",
		            rel->parent->id, rel->id, key->key);
	} else {
		abort();
	}
	dst_rel_cdc(rel, key);
	dst_rel_delete(rel, key);
	dst_key_free(key);
}

static void
dst_begin(DstUser* self)
{
	auto log = &self->log;
	dst_log_reset(log);

	// save cdc metrics
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if (rel->type != DST_REL_SUBSCRIPTION)
			continue;
		rel->step_cdc_sum   = rel->cdc_sum;
		rel->step_cdc_count = rel->cdc_count;
	}
}

static void
dst_rollback(DstUser* self)
{
	auto log = &self->log;
	for (auto i = log->ops_count - 1; i >= 0; i--)
	{
		auto op = dst_log_at(log, i);
		switch (op->op) {
		case DST_OP_PUBLISH:
		{
			// nothing
			break;
		}
		case DST_OP_INSERT:
		{
			// delete (table, table_vector)
			auto ref = dst_rel_get(op->rel, op->key.key);
			assert(ref);
			dst_rel_delete(op->rel, ref);
			dst_key_free(ref);
			break;
		}
		case DST_OP_UPSERT:
		case DST_OP_UPDATE:
		{
			// replace (table, table_vector)
			auto ref = dst_rel_get(op->rel, op->key.key);
			assert(ref);
			if (op->rel->type == DST_REL_TABLE) {
				ref->value = op->prev.value;
			} else {
				ref->value_vector[0] = op->prev.value_vector[0];
				ref->value_vector[1] = op->prev.value_vector[1];
				ref->value_vector[2] = op->prev.value_vector[2];
				ref->value_vector[3] = op->prev.value_vector[3];
			}
			break;
		}
		case DST_OP_DELETE:
		{
			// insert (table, table_vector)
			auto ref = dst_key_allocate(op->prev.key);
			dst_rel_set(op->rel, ref);
			if (op->rel->type == DST_REL_TABLE) {
				ref->value = op->prev.value;
			} else {
				ref->value_vector[0] = op->prev.value_vector[0];
				ref->value_vector[1] = op->prev.value_vector[1];
				ref->value_vector[2] = op->prev.value_vector[2];
				ref->value_vector[3] = op->prev.value_vector[3];
			}
			break;
		}
		}
	}

	// restore cdc metrics
	list_foreach(&self->rels)
	{
		auto rel = list_at(DstRel, link);
		if (rel->type != DST_REL_SUBSCRIPTION)
			continue;
		rel->cdc_sum   = rel->step_cdc_sum;
		rel->cdc_count = rel->step_cdc_count;
	}

	dst_log_reset(log);
}

void
dst_step_ddl(DstUser* self)
{
	info("[{u64}] DDL", self->dst->step);

	auto create = 0;
	if (self->rels_count == 0)
		create = 1;
	else
	if (self->rels_count == (int)self->dst->opt_rels.integer)
		create = 0;
	else
		create = random_generate(&am_task->random) % 2;

	if (create)
	{
		// create any relation
		auto type = random_generate(&am_task->random) % DST_REL_MAX;
		if (type == DST_REL_SUBSCRIPTION)
		{
			// create subscription for table, topic
			auto count = dst_user_count(self, true, true, true, true);
			if (! count)
			{
				dst_user_create(self, DST_REL_TABLE);
				return;
			}

			// randomly choose table or topic (caped by the count)
			auto pos = random_generate(&am_task->random) % count;
			auto parent = dst_user_rel_filter(self, pos, true, true, true, true);
			assert(parent);
			dst_user_create_for(self, parent, type);

		} else
		if (type == DST_REL_CLONE)
		{
			// create table clone
			auto count = dst_user_count(self, true, false, false, false);
			if (! count)
			{
				dst_user_create(self, DST_REL_TABLE);
				return;
			}

			// randomly choose table
			auto pos = random_generate(&am_task->random) % count;
			auto parent = dst_user_rel_filter(self, pos, true, false, false, false);
			assert(parent);
			dst_user_create_for(self, parent, type);
		} else
		{
			dst_user_create(self, type);
		}
	} else
	{
		// drop any relation
		auto rel_order = random_generate(&am_task->random) % self->rels_count;
		auto rel = dst_user_rel(self, rel_order);
		dst_user_drop(self, rel);
	}
}

void
dst_step_ddl_user(DstUser* self)
{
	auto dst = self->dst;
	info("[{u64}] DDL USER", dst->step);

	// connect (superuser)
	char path[PATH_MAX];
	format(path, sizeof(path), "{str}/env", opt_string_of(&dst->opt_dir));

	Endpoint endpoint;
	endpoint_init(&endpoint);
	defer(endpoint_free, &endpoint);
	opt_string_set_cstr(&endpoint.path, path);

	auto client = client_allocate();
	defer(client_free, client);
	client_set_endpoint(client, &endpoint);
	client_connect(client);

	// create or drop
	auto create = 0;
	if (dst->users_count == 1)
		create = 1;
	else
	if (dst->users_count == (int)dst->opt_users.integer)
		create = 0;
	else
		create = random_generate(&am_task->random) % 2;

	if (create)
	{
		// create new user
		auto user = dst_user_allocate(dst, dst->users_seq++);
		list_append(&dst->users, &user->link);
		dst->users_count++;

		dst_execute(dst, client, "CREATE USER user_{u64}", user->id);
		dst_user_connect(user);

		// (created empty)

	} else
	{
		// drop any user
		auto user_order = random_generate(&am_task->random) % dst->users_count;
		auto user = dst_user(dst, user_order);
		dst_execute(dst, client, "DROP USER user_{u64} CASCADE",
		            user->id);

		list_unlink(&user->link);
		dst->users_count--;
		dst_user_free(user);
	}
}

void
dst_step(DstUser* self)
{
	dst_begin(self);

	// generate single or multi-stmts
	int stmts = random_generate(&am_task->random) % 9 + 1;
	auto error_inject = (random_generate(&am_task->random) % 100) < 10;
	if (stmts == 1)
	{
		dst_stmt(self);
		if (error_inject)
			buf_format(&self->log.sql, "SELECT error('injected');");
	} else
	{
		auto error_at = -1;
		if (error_inject)
			error_at = random_generate(&am_task->random) % stmts;
		buf_format(&self->log.sql, "BEGIN;");
		for (auto i = 0; i < stmts; i++)
		{
			dst_stmt(self);
			if (i == error_at)
				buf_format(&self->log.sql, "SELECT error('injected');");
		}
		buf_format(&self->log.sql, "END;");
	}

	// execute (rollback state on failure)
	if (! dst_execute_log(self))
		dst_rollback(self);
}
