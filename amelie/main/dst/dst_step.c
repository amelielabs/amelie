

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

	// generate relation (table, table_vector, topic)
	auto rel_id = random_generate(&am_task->random) % DST_REL_MAX;
	auto rel = &self->rels[rel_id];
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
		            "PUBLISH INTO dst_topic [{u64}, {i64}];",
		            key.key, key.value);

		// add cdc event
		dst_rel_event_add(rel, DST_EVENT_PUBLISH, &key);
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
			            "INSERT INTO dst_table VALUES ({u64}, {i64});",
			            key->key, key->value);
		} else
		{
			// table_vector
			key->value_vector[0] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[1] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[2] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[3] = random_generate_fp(&am_task->random, -1.0, 1.0);
			buf_format(&self->log.sql,
			            "INSERT INTO dst_table_vector VALUES ({u64}, vector [{f}, {f}, {f}, {f}]);",
			            key->key,
			            key->value_vector[0],
			            key->value_vector[1],
			            key->value_vector[2],
			            key->value_vector[3]);
		}
		dst_rel_event_add(rel, DST_EVENT_REPLACE, key);
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
			            "INSERT INTO dst_table VALUES ({u64}, null) ON CONFLICT DO UPDATE SET state = {i64};",
			            key->key, key->value);
		} else
		{
			key->value_vector[0] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[1] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[2] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[3] = random_generate_fp(&am_task->random, -1.0, 1.0);
			buf_format(&self->log.sql,
			           "INSERT INTO dst_table_vector VALUES ({u64}, null) "
			           "ON CONFLICT DO UPDATE SET state = vector [{f}, {f}, {f}, {f}];",
			           key->key,
			           key->value_vector[0],
			           key->value_vector[1],
			           key->value_vector[2],
			           key->value_vector[3]);
		}
		dst_rel_event_add(rel, DST_EVENT_REPLACE, key);

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
			            "UPDATE dst_table SET state = {i64} WHERE id = {i64};",
			            key->value, key->key);
		} else
		{
			key->value_vector[0] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[1] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[2] = random_generate_fp(&am_task->random, -1.0, 1.0);
			key->value_vector[3] = random_generate_fp(&am_task->random, -1.0, 1.0);
			buf_format(&self->log.sql,
			           "UPDATE dst_table_vector "
			           "SET state = vector [{f}, {f}, {f}, {f}] WHERE id = {u64};",
			           key->value_vector[0],
			           key->value_vector[1],
			           key->value_vector[2],
			           key->value_vector[3],
			           key->key);
		}
		dst_rel_event_add(rel, DST_EVENT_REPLACE, key);
		op->key = *key;
		return;
	}

	// DELETE (20%)
	op->op = DST_OP_DELETE;
	if (rel->type == DST_REL_TABLE)
	{
		buf_format(&self->log.sql,
		           "DELETE FROM dst_table WHERE id = {u64};",
		            key->key);
	} else
	{
		buf_format(&self->log.sql,
		           "DELETE FROM dst_table_vector WHERE id = {u64};",
		           key->key);
	}
	dst_rel_event_add(rel, DST_EVENT_DELETE, key);
	dst_rel_delete(rel, key);
	dst_key_free(key);
}

void
dst_step(DstUser* self)
{
	dst_log_reset(&self->log);
	buf_format(&self->log.sql, "BEGIN;");
	dst_stmt(self);
	buf_format(&self->log.sql, "END;");

	dst_execute_log(self);
		// todo: rollback
}
