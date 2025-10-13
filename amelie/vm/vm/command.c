
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
#include <amelie_vm.h>

hot void
csend_shard(Vm* self, Op* op)
{
	// [start, table, store, result, refs]
	auto dtr   = self->dtr;
	auto table = (Table*)op->b;
	auto keys  = table_keys(table);

	// create dispatch
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (op->d != -1)
	{
		auto result = (Union*)reg_at(&self->r, op->d)->store;
		result->dispatch = dispatch;
		dispatch_set_returning(dispatch);
	}
	if (self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// redistribute rows between backends
	Req* map[dtr->dispatch_mgr.ctrs_count];
	memset(map, 0, sizeof(map));

	auto refs  = stack_at(&self->stack, op->e);
	auto store = reg_at(&self->r, op->c)->store;
	if (store->type == STORE_SET)
	{
		auto set = (Set*)store;
		for (auto order = 0; order < set->count_rows; order++)
		{
			auto row      = set_row(set, order);
			auto identity = row_get_identity(table, refs, row);
			auto hash     = row_value_hash(keys, refs, row, identity);
			auto part     = part_map_get(&table->part_list.map, hash);

			auto req = map[part->core->order];
			if (req == NULL)
			{
				req = dispatch_add(dispatch, &dispatch_mgr->cache_req,
				                   REQ_EXECUTE,
				                   op->a,
				                   &self->program->code_backend,
				                   &self->program->code_data,
				                   part->core);
				if (op->e > 0)
					req_copy_refs(req, refs, op->e);
				map[part->core->order] = req;
			}

			buf_write(&req->arg, &row, sizeof(Value*));
			buf_write(&req->arg, &identity, sizeof(int64_t));
		}
	} else
	{
		auto it = store_iterator(store);
		defer(store_iterator_close, it);
		Value* row;
		for (; (row = store_iterator_at(it)); store_iterator_next(it))
		{
			auto identity = row_get_identity(table, refs, row);
			auto hash     = row_value_hash(keys, refs, row, identity);
			auto part     = part_map_get(&table->part_list.map, hash);

			auto req  = map[part->core->order];
			if (req == NULL)
			{
				req = dispatch_add(dispatch, &dispatch_mgr->cache_req,
				                   REQ_EXECUTE,
				                   op->a,
				                   &self->program->code_backend,
				                   &self->program->code_data,
				                   part->core);
				if (op->e > 0)
					req_copy_refs(req, refs, op->e);
				map[part->core->order] = req;
			}
			buf_write(&req->arg, &row, sizeof(Value*));
			buf_write(&req->arg, &identity, sizeof(int64_t));
		}
	}
	if (op->e > 0)
		stack_popn(&self->stack, op->e);

	executor_send(share()->executor, dtr, dispatch);
	value_free(reg_at(&self->r, op->c));
}

hot void
csend_lookup(Vm* self, Op* op)
{
	// [start, table, hash, result, refs]
	auto dtr   = self->dtr;
	auto table = (Table*)op->b;

	// create dispatch
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (op->d != -1)
	{
		auto result = (Union*)reg_at(&self->r, op->d)->store;
		result->dispatch = dispatch;
		dispatch_set_returning(dispatch);
	}
	if (self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// shard by precomputed hash
	auto part = part_map_get(&table->part_list.map, op->c);
	auto req  = dispatch_add(dispatch, &dispatch_mgr->cache_req,
	                         REQ_EXECUTE,
	                         op->a,
	                         &self->program->code_backend,
	                         &self->program->code_data,
	                         part->core);
	if (op->e > 0)
	{
		req_copy_refs(req, stack_at(&self->stack, op->e), op->e);
		stack_popn(&self->stack, op->e);
	}

	executor_send(share()->executor, dtr, dispatch);
}

hot void
csend_lookup_by(Vm* self, Op* op)
{
	// [start, table, result, refs]
	auto dtr   = self->dtr;
	auto table = (Table*)op->b;
	auto index = table_primary(table);

	// create dispatch
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (op->c != -1)
	{
		auto result = (Union*)reg_at(&self->r, op->c)->store;
		result->dispatch = dispatch;
		dispatch_set_returning(dispatch);
	}
	if (self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// compute hash using key values
	uint32_t hash = 0;
	list_foreach(&index->keys.list)
	{
		auto key = list_at(Key, link);
		auto value = stack_at(&self->stack, index->keys.list_count - key->order);
		hash = value_hash(value, key->column->type_size, hash);
	}
	stack_popn(&self->stack, index->keys.list_count);

	auto part = part_map_get(&table->part_list.map, hash);
	auto req  = dispatch_add(dispatch, &dispatch_mgr->cache_req,
	                         REQ_EXECUTE,
	                         op->a,
	                         &self->program->code_backend,
	                         &self->program->code_data,
	                         part->core);
	if (op->d > 0)
	{
		req_copy_refs(req, stack_at(&self->stack, op->d), op->d);
		stack_popn(&self->stack, op->d);
	}

	executor_send(share()->executor, dtr, dispatch);
}

hot void
csend_all(Vm* self, Op* op)
{
	// [start, table, result, refs]
	auto table = (Table*)op->b;
	auto dtr = self->dtr;

	// create dispatch
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (op->c != -1)
	{
		auto result = (Union*)reg_at(&self->r, op->c)->store;
		result->dispatch = dispatch;
		dispatch_set_returning(dispatch);
	}
	if (self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// send to all table backends
	list_foreach(&table->part_list.list)
	{
		auto part = list_at(Part, link);
		auto req = dispatch_add(dispatch, &dispatch_mgr->cache_req,
		                        REQ_EXECUTE,
		                        op->a,
		                        &self->program->code_backend,
		                        &self->program->code_data,
		                        part->core);
		if (op->d > 0)
			req_copy_refs(req, stack_at(&self->stack, op->d), op->d);
	}
	if (op->d > 0)
		stack_popn(&self->stack, op->d);

	executor_send(share()->executor, dtr, dispatch);
}

void
cclose(Vm* self, Op* op)
{
	unused(op);
	dispatch_mgr_close(&self->dtr->dispatch_mgr);
}

hot void
cunion_set(Vm* self, Op* op)
{
	// [union, distinct, limit, offset]
	auto ref = (Union*)reg_at(&self->r, op->a)->store;

	// distinct
	bool distinct = op->b;

	// limit
	int64_t limit = INT64_MAX;
	if (op->c != -1)
	{
		if (unlikely(reg_at(&self->r, op->c)->type != TYPE_INT))
			error("LIMIT: integer type expected");
		limit = reg_at(&self->r, op->c)->integer;
		if (unlikely(limit < 0))
			error("LIMIT: positive integer value expected");
	}

	// offset
	int64_t offset = 0;
	if (op->d != -1)
	{
		if (unlikely(reg_at(&self->r, op->d)->type != TYPE_INT))
			error("OFFSET: integer type expected");
		offset = reg_at(&self->r, op->d)->integer;
		if (unlikely(offset < 0))
			error("OFFSET: positive integer value expected");
	}

	union_set(ref, distinct, limit, offset);
}

hot void
cunion_recv(Vm* self, Op* op)
{
	// [union]
	auto result = (Union*)reg_at(&self->r, op->a)->store;

	auto dispatch = (Dispatch*)result->dispatch;
	assert(dispatch);

	// wait for group completion
	dispatch_wait(dispatch);

	// add results to the union
	auto error = (Req*)NULL;
	list_foreach(&dispatch->list)
	{
		auto req = list_at(Req, link);
		if (req->error && !error)
			error = req;

		auto value = &req->result;
		if (value->type == TYPE_STORE)
		{
			auto set = (Set*)value->store;
			// create child union out of set childs
			if (set->child)
			{
				if (! result->child)
				{
					auto ref = union_create();
					union_set(ref, true, INT64_MAX, 0);
					union_assign(result, ref);
				}
				auto child = set->child;
				set_assign(set, NULL);
				union_add(result->child, child);
			}
			union_add(result, set);
			value_reset(value);
		}
	}
	if (error)
		rethrow_buf(error->error);
}

void
cassign(Vm* self, Op* op)
{
	// [result, value, column]
	auto dst = reg_at(&self->r, op->a);
	value_free(dst);

	auto src = reg_at(&self->r, op->b);
	if (src->type == TYPE_STORE)
	{
		auto store = src->store;
		auto it = store_iterator(store);
		defer(store_iterator_close, it);
		if (store_iterator_has(it))
			value_copy(dst, it->current + op->c);
		else
			value_set_null(dst);
	} else {
		value_copy(dst, src);
	}
}

hot Op*
ctable_open(Vm* self, Op* op, bool point_lookup, bool open_part)
{
	// [cursor, name_offset, _eof, keys_count]

	// read names
	uint8_t* pos = code_data_at(self->code_data, op->b);
	Str name_schema;
	Str name_table;
	Str name_index;
	json_read_string(&pos, &name_schema);
	json_read_string(&pos, &name_table);
	json_read_string(&pos, &name_index);

	// find table, partition and index
	auto table = table_mgr_find(&share()->db->catalog.table_mgr, &name_schema, &name_table, true);
	auto index = table_find_index(table, &name_index, true);
	auto keys  = &index->keys;
	auto keys_count = op->d;

	// create cursor key
	auto buf = buf_create();
	defer_buf(buf);
	auto key = row_create_key(buf, keys, stack_at(&self->stack, keys_count), keys_count);
	stack_popn(&self->stack, keys_count);

	// in case of hash index, use key only for point-lookup
	auto key_ref = key;
	if (index->type == INDEX_HASH && !point_lookup)
		key_ref = NULL;

	// open cursor
	auto cursor = reg_at(&self->r, op->a);
	if (open_part)
		cursor->part = part_list_match(&table->part_list, self->core);
	else
		cursor->part = NULL;
	cursor->cursor = part_list_iterator(&table->part_list, cursor->part, index, point_lookup, key_ref);
	cursor->table  = table;
	cursor->type   = TYPE_CURSOR;

	// jmp to next op if has data
	if (likely(iterator_has(cursor->cursor)))
		return ++op;

	// jmp on eof
	return code_at(self->code, op->c);
}

hot Op*
ctable_open_heap(Vm* self, Op* op)
{
	// [cursor, name_offset, _eof]

	// read names
	uint8_t* pos = code_data_at(self->code_data, op->b);
	Str name_schema;
	Str name_table;
	json_read_string(&pos, &name_schema);
	json_read_string(&pos, &name_table);

	// find table and partition
	auto table = table_mgr_find(&share()->db->catalog.table_mgr, &name_schema, &name_table, true);
	auto part  = part_list_match(&table->part_list, self->core);

	// open cursor
	auto cursor = reg_at(&self->r, op->a);
	cursor->table  = table;
	cursor->part   = part;
	cursor->cursor = heap_iterator_allocate(&part->heap);
	iterator_open(cursor->cursor, NULL);
	cursor->type   = TYPE_CURSOR;

	// jmp to next op if has data
	if (likely(iterator_has(cursor->cursor)))
		return ++op;

	// jmp on eof
	return code_at(self->code, op->c);
}

hot void
ctable_prepare(Vm* self, Op* op)
{
	// [cursor, table*]

	// find partition
	Table* table = (Table*)op->b;

	// prepare cursor and primary index iterator for related partition
	auto cursor = reg_at(&self->r, op->a);
	cursor->table  = table;
	cursor->part   = part_list_match(&table->part_list, self->core);
	cursor->cursor = index_iterator(part_primary(cursor->part));
	cursor->type   = TYPE_CURSOR;

	// prepare upsert state
	self->upsert   = self->code_arg->start;
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table*]

	// find related table partition
	auto table   = (Table*)op->a;
	auto part    = part_list_match(&table->part_list, self->core);
	auto columns = table_columns(table);

	// insert
	auto pos = self->code_arg->start;
	auto end = self->code_arg->position;
	while (pos < end)
	{
		auto value    = *(Value**)pos;
		auto identity = *(int64_t*)(pos + sizeof(Value*));
		pos += sizeof(Value*) + sizeof(int64_t);

		auto row = row_create(&part->heap, columns, value, self->refs, identity);
		part_insert(part, self->tr, false, row);
	}
}

Op*
cupsert(Vm* self, Op* op)
{
	// [cursor, _jmp, _jmp_returning]
	auto cursor = reg_at(&self->r, op->a);
	assert(cursor->type == TYPE_CURSOR);

	auto columns = table_columns(cursor->table);
	auto end = self->code_arg->position;
	while (self->upsert < end)
	{
		// set cursor ref pointer to the current insert row
		auto value    = *(Value**)self->upsert;
		auto identity = *(int64_t*)(self->upsert + sizeof(Value*));
		self->upsert += sizeof(Value*) + sizeof(int64_t);

		auto row = row_create(&cursor->part->heap, columns, value, self->refs, identity);

		// insert or get (open iterator in both cases)
		auto exists = part_upsert(cursor->part, self->tr, cursor->cursor, row);
		if (exists)
		{
			// upsert

			// execute on where/on conflict logic (and returning after it)
			return code_at(self->code, op->b);
		}

		// returning
		if (op->c != -1)
		{
			// jmp to returning
			return code_at(self->code, op->c);
		}
	}

	// done
	return ++op;
}

hot void
cdelete(Vm* self, Op* op)
{
	// [cursor]
	auto cursor = reg_at(&self->r, op->a);
	part_delete(cursor->part, self->tr, cursor->cursor);
}

hot void
cupdate(Vm* self, Op* op)
{
	// [cursor, order/value count]
	auto cursor = reg_at(&self->r, op->a);
	assert(cursor->type == TYPE_CURSOR);
	auto row_src = iterator_at(cursor->cursor);
	auto row_values = stack_at(&self->stack, op->b * 2);
	auto row = row_update(&cursor->part->heap, row_src, table_columns(cursor->table),
	                      row_values, op->b);
	part_update(cursor->part, self->tr, cursor->cursor, row);
	stack_popn(&self->stack, op->b * 2);
}

hot void
cupdate_store(Vm* self, Op* op)
{
	// [cursor, count]
	auto cursor = reg_at(&self->r, op->a);
	assert(cursor->type == TYPE_CURSOR_STORE);
	auto count = op->b * 2;
	auto row = store_iterator_at(cursor->cursor_store);
	auto values = stack_at(&self->stack, count);

	// [column_order, value], ...
	for (auto order = 0; order < count; order += 2)
		value_move(&row[values[order].integer], &values[order + 1]);
}
