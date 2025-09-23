
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
	// [start, table, store, result]
	auto dtr   = self->dtr;
	auto table = (Table*)op->b;
	auto keys  = table_keys(table);

	// get result union
	Union* result = NULL;
	if (op->d != -1)
		result = (Union*)reg_at(&self->r, op->d)->store;

	// redistribute rows between backends
	Req* map[dtr->dispatch.ctrs_count];
	memset(map, 0, sizeof(map));

	ReqList list;
	req_list_init(&list);

	auto store = reg_at(&self->r, op->c)->store;
	if (store->type == STORE_SET)
	{
		auto set = (Set*)store;
		for (auto order = 0; order < set->count_rows; order++)
		{
			auto row  = set_row(set, order);
			auto hash = row_value_hash(keys, row);
			auto part = part_map_get(&table->part_list.map, hash);
			auto req  = map[part->core->order];
			if (req == NULL)
			{
				req = req_create(&dtr->dispatch.req_cache);
				req->type      = REQ_EXECUTE;
				req->start     = op->a;
				req->code      = &self->program->code_backend;
				req->code_data = &self->program->code_data;
				req->regs      = &self->r;
				req->args      = self->args;
				req->core      = part->core;
				req_list_add(&list, req);
				map[part->core->order] = req;
				if (result)
				{
					req->result_pending = true;
					list_append(&result->list_reqs, &req->link_union);
					result->list_reqs_count++;
				}
			}
			buf_write(&req->arg, &row, sizeof(Value*));
		}
	} else
	{
		auto it = store_iterator(store);
		defer(store_iterator_close, it);
		Value* row;
		for (; (row = store_iterator_at(it)); store_iterator_next(it))
		{
			auto hash = row_value_hash(keys, row);
			auto part = part_map_get(&table->part_list.map, hash);
			auto req  = map[part->core->order];
			if (req == NULL)
			{
				req = req_create(&dtr->dispatch.req_cache);
				req->type      = REQ_EXECUTE;
				req->start     = op->a;
				req->code      = &self->program->code_backend;
				req->code_data = &self->program->code_data;
				req->regs      = &self->r;
				req->args      = self->args;
				req->core      = part->core;
				req_list_add(&list, req);
				map[part->core->order] = req;
				if (result)
				{
					req->result_pending = true;
					list_append(&result->list_reqs, &req->link_union);
					result->list_reqs_count++;
				}
			}
			buf_write(&req->arg, &row, sizeof(Value*));
		}
	}

	auto is_last = self->program->send_last == code_posof(self->code, op);
	executor_send(share()->executor, dtr, &list, is_last);
	value_free(reg_at(&self->r, op->c));
}

hot void
csend_lookup(Vm* self, Op* op)
{
	// [start, table, hash, result]
	auto dtr   = self->dtr;
	auto table = (Table*)op->b;

	// get result union
	Union* result = NULL;
	if (op->d != -1)
		result = (Union*)reg_at(&self->r, op->d)->store;

	// shard by precomputed hash
	ReqList list;
	req_list_init(&list);
	auto part = part_map_get(&table->part_list.map, op->c);
	auto req = req_create(&dtr->dispatch.req_cache);
	req->type      = REQ_EXECUTE;
	req->start     = op->a;
	req->code      = &self->program->code_backend;
	req->code_data = &self->program->code_data;
	req->regs      = &self->r;
	req->args      = self->args;
	req->core      = part->core;
	req_list_add(&list, req);
	if (result)
	{
		req->result_pending = true;
		list_append(&result->list_reqs, &req->link_union);
		result->list_reqs_count++;
	}

	auto is_last = self->program->send_last == code_posof(self->code, op);
	executor_send(share()->executor, dtr, &list, is_last);
}

hot void
csend_lookup_by(Vm* self, Op* op)
{
	// [start, table, result]
	auto dtr   = self->dtr;
	auto table = (Table*)op->b;
	auto index = table_primary(table);

	// get result union
	Union* result = NULL;
	if (op->c != -1)
		result = (Union*)reg_at(&self->r, op->c)->store;

	// compute hash using key values
	uint32_t hash = 0;
	list_foreach(&index->keys.list)
	{
		auto key = list_at(Key, link);
		auto value = stack_at(&self->stack, index->keys.list_count - key->order);
		hash = value_hash(value, key->column->type_size, hash);
	}
	stack_popn(&self->stack, index->keys.list_count);

	ReqList list;
	req_list_init(&list);
	auto part = part_map_get(&table->part_list.map, hash);
	auto req = req_create(&dtr->dispatch.req_cache);
	req->type      = REQ_EXECUTE;
	req->start     = op->a;
	req->code      = &self->program->code_backend;
	req->code_data = &self->program->code_data;
	req->regs      = &self->r;
	req->args      = self->args;
	req->core      = part->core;
	req_list_add(&list, req);
	if (result)
	{
		req->result_pending = true;
		list_append(&result->list_reqs, &req->link_union);
		result->list_reqs_count++;
	}

	auto is_last = self->program->send_last == code_posof(self->code, op);
	executor_send(share()->executor, dtr, &list, is_last);
}

hot void
csend_all(Vm* self, Op* op)
{
	// [start, table, result]
	auto table = (Table*)op->b;
	auto dtr = self->dtr;

	// get result union
	Union* result = NULL;
	if (op->c != -1)
		result = (Union*)reg_at(&self->r, op->c)->store;

	// send to all table backends
	ReqList list;
	req_list_init(&list);
	list_foreach(&table->part_list.list)
	{
		auto part = list_at(Part, link);
		auto req = req_create(&dtr->dispatch.req_cache);
		req->type      = REQ_EXECUTE;
		req->start     = op->a;
		req->code      = &self->program->code_backend;
		req->code_data = &self->program->code_data;
		req->regs      = &self->r;
		req->args      = self->args;
		req->core      = part->core;
		req_list_add(&list, req);
		if (result)
		{
			req->result_pending = true;
			list_append(&result->list_reqs, &req->link_union);
			result->list_reqs_count++;
		}
	}

	auto is_last = self->program->send_last == code_posof(self->code, op);
	executor_send(share()->executor, dtr, &list, is_last);
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

	// wait for all requests completion and add result to the union
	auto error = (Req*)NULL;
	list_foreach(&result->list_reqs)
	{
		auto req = list_at(Req, link_union);
		event_wait(&req->result_ready, -1);
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
	// [result, store]
	auto store = reg_at(&self->r, op->b)->store;
	auto it = store_iterator(store);
	defer(store_iterator_close, it);
	auto dst = reg_at(&self->r, op->a);
	if (store_iterator_has(it))
		value_copy(dst, it->current);
	else
		value_set_null(dst);
}

hot Op*
ctable_open(Vm* self, Op* op, bool point_lookup, bool open_part)
{
	// [cursor, name_offset, _eof, keys_count]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

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
	if (open_part)
		cursor->part = part_list_match(&table->part_list, self->core);
	else
		cursor->part = NULL;
	cursor->it    = part_list_iterator(&table->part_list, cursor->part, index, point_lookup, key_ref);
	cursor->table = table;
	cursor->type  = CURSOR_TABLE;

	// jmp to next op if has data
	if (likely(iterator_has(cursor->it)))
		return ++op;

	// jmp on eof
	return code_at(self->code, op->c);
}

hot Op*
ctable_open_heap(Vm* self, Op* op)
{
	// [cursor, name_offset, _eof]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

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
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->part  = part;
	cursor->it    = heap_iterator_allocate(&part->heap);
	iterator_open(cursor->it, NULL);

	// jmp to next op if has data
	if (likely(iterator_has(cursor->it)))
		return ++op;

	// jmp on eof
	return code_at(self->code, op->c);
}

hot void
ctable_prepare(Vm* self, Op* op)
{
	// [target_id, table*]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// find partition
	Table* table = (Table*)op->b;

	// prepare cursor and primary index iterator for related partition
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->part  = part_list_match(&table->part_list, self->core);
	cursor->it    = index_iterator(part_primary(cursor->part));
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
	auto list       = (Value**)self->code_arg->start;
	int  list_count = buf_size(self->code_arg) / sizeof(Value*);
	for (int i = 0; i < list_count; i++)
	{
		auto row = row_create(&part->heap, columns, list[i]);
		part_insert(part, self->tr, false, row);
	}
}

hot Op*
cupsert(Vm* self, Op* op)
{
	// [target_id, _jmp, _jmp_returning]
	auto cursor  = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto columns = table_columns(cursor->table);

	// first call
	if (cursor->ref_pos == 0)
		cursor->ref_count = buf_size(self->code_arg) / sizeof(Value*);

	// insert or upsert
	auto list = (Value**)self->code_arg->start;
	while (cursor->ref_pos < cursor->ref_count)
	{
		// set cursor ref pointer to the current insert row
		cursor->ref = list[cursor->ref_pos];
		cursor->ref_pos++;

		auto row = row_create(&cursor->part->heap, columns, cursor->ref);

		// insert or get (open iterator in both cases)
		auto exists = part_upsert(cursor->part, self->tr, cursor->it, row);
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
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	part_delete(cursor->part, self->tr, cursor->it);
}

hot void
cupdate(Vm* self, Op* op)
{
	// [cursor, order/value count]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto row_src = iterator_at(cursor->it);
	auto row_values = stack_at(&self->stack, op->b * 2);
	auto row = row_update(&cursor->part->heap, row_src, table_columns(cursor->table),
	                      row_values, op->b);
	part_update(cursor->part, self->tr, cursor->it, row);
	stack_popn(&self->stack, op->b * 2);
}

hot void
cupdate_store(Vm* self, Op* op)
{
	// [cursor, count]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	assert(cursor->type == CURSOR_STORE);
	auto count = op->b * 2;
	auto row = store_iterator_at(cursor->it_store);
	auto values = stack_at(&self->stack, count);

	// [column_order, value], ...
	for (auto order = 0; order < count; order += 2)
		value_move(&row[values[order].integer], &values[order + 1]);
}
