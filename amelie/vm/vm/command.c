
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
#include <amelie_io.h>
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
	// [stmt, start, table, store]
	auto dtr   = self->dtr;
	auto table = (Table*)op->c;
	auto keys  = table_keys(table);

	// redistribute rows between backends
	Req* map[table->config->partitions_count];
	memset(map, 0, sizeof(map));

	ReqList list;
	req_list_init(&list);

	auto store = reg_at(&self->r, op->d)->store;
	if (store->type == STORE_SET)
	{
		auto set = (Set*)store;
		for (auto order = 0; order < set->count_rows; order++)
		{
			auto row  = set_row(set, order);
			auto hash = row_value_hash(keys, row);
			auto part = part_map_get(&table->part_list.map, hash);
			auto req  = map[part->order];
			if (! req)
			{
				req = req_create(&self->executor->req_mgr);
				req->arg.type    = REQ_EXECUTE;
				req->arg.start   = op->b;
				req->arg.backlog = &part->backlog;
				req_list_add(&list, req);
				map[part->order] = req;
			}
			buf_write(&req->arg.arg, &row, sizeof(Value*));
		}
	} else
	{
		auto it = store_iterator(store);
		defer(store_iterator_close, it);
		Value* row;
		for (; (row = store_iterator_at(it)); store_iterator_next(it))
		{
			auto hash  = row_value_hash(keys, row);
			auto part = part_map_get(&table->part_list.map, hash);
			auto req  = map[part->order];
			if (! req)
			{
				req = req_create(&self->executor->req_mgr);
				req->arg.type    = REQ_EXECUTE;
				req->arg.start   = op->b;
				req->arg.backlog = &part->backlog;
				req_list_add(&list, req);
				map[part->order] = req;
			}
			buf_write(&req->arg.arg, &row, sizeof(Value*));
		}
	}

	executor_send(self->executor, dtr, op->a, &list);
	value_free(reg_at(&self->r, op->d));
}

hot void
csend_lookup(Vm* self, Op* op)
{
	// [stmt, start, table, hash]
	auto dtr   = self->dtr;
	auto table = (Table*)op->c;

	// shard by precomputed hash
	ReqList list;
	req_list_init(&list);
	auto part = part_map_get(&table->part_list.map, op->d);
	auto req = req_create(&self->executor->req_mgr);
	req->arg.type    = REQ_EXECUTE;
	req->arg.start   = op->b;
	req->arg.backlog = &part->backlog;
	req_list_add(&list, req);

	executor_send(self->executor, dtr, op->a, &list);
}

hot void
csend_all(Vm* self, Op* op)
{
	// [stmt, start, table]
	auto table = (Table*)op->c;

	// send to all table partitiions
	ReqList list;
	req_list_init(&list);
	list_foreach(&table->part_list.list)
	{
		auto part = list_at(Part, link);
		auto req = req_create(&self->executor->req_mgr);
		req->arg.type    = REQ_EXECUTE;
		req->arg.start   = op->b;
		req->arg.backlog = &part->backlog;
		req_list_add(&list, req);
	}

	executor_send(self->executor, self->dtr, op->a, &list);
}

hot void
crecv(Vm* self, Op* op)
{
	// [stmt]
	executor_recv(self->executor, self->dtr, op->a);
}

hot void
crecv_to(Vm* self, Op* op)
{
	// [result, stmt]
	executor_recv(self->executor, self->dtr, op->b);

	auto step = dispatch_at(&self->dtr->dispatch, op->b);
	auto req  = container_of(list_first(&step->list.list), Req, link);
	value_move(reg_at(&self->r, op->a), &req->arg.result);
}

hot void
cunion(Vm* self, Op* op)
{
	// [union, set, limit, offset]

	// distinct
	bool distinct = stack_at(&self->stack, 1)->integer;
	stack_popn(&self->stack, 1);

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

	// create union object
	auto ref = union_create(distinct, limit, offset);
	value_set_store(reg_at(&self->r, op->a), &ref->store);

	// add set
	auto value = reg_at(&self->r, op->b);
	union_add(ref, (Set*)value->store);
	value_reset(value);
}

hot void
cunion_recv(Vm* self, Op* op)
{
	// [union, limit, offset, stmt]

	// distinct
	bool distinct = stack_at(&self->stack, 1)->integer;
	stack_popn(&self->stack, 1);

	// limit
	int64_t limit = INT64_MAX;
	if (op->b != -1)
	{
		if (unlikely(reg_at(&self->r, op->b)->type != TYPE_INT))
			error("LIMIT: integer type expected");
		limit = reg_at(&self->r, op->b)->integer;
		if (unlikely(limit < 0))
			error("LIMIT: positive integer value expected");
	}

	// offset
	int64_t offset = 0;
	if (op->c != -1)
	{
		if (unlikely(reg_at(&self->r, op->c)->type != TYPE_INT))
			error("OFFSET: integer type expected");
		offset = reg_at(&self->r, op->c)->integer;
		if (unlikely(offset < 0))
			error("OFFSET: positive integer value expected");
	}

	// create union object
	auto ref = union_create(distinct, limit, offset);
	value_set_store(reg_at(&self->r, op->a), &ref->store);

	// add result sets to the union
	auto step = dispatch_at(&self->dtr->dispatch, op->d);
	list_foreach(&step->list.list)
	{
		auto req = list_at(Req, link);
		auto value = &req->arg.result;
		if (value->type == TYPE_STORE)
		{
			auto set = (Set*)value->store;
			// create child union out of set childs
			if (set->child)
			{
				if (! ref->child)
					union_assign(ref, union_create(true, INT64_MAX, 0));
				auto child = set->child;
				set_assign(set, NULL);
				union_add(ref->child, child);
			}
			union_add(ref, set);
			value_reset(value);
		}
	}
}

hot Op*
ctable_open(Vm* self, Op* op)
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
	auto table = table_mgr_find(&self->db->table_mgr, &name_schema, &name_table, true);
	Part* part;
	if (table->config->shared)
		part = container_of(table->part_list.list.next, Part, link);
	else
		part = self->part;
	assert(part);
	auto index = part_find(part, &name_index, true);
	auto keys  = index_keys(index);
	auto keys_count = op->d;

	// create cursor key
	auto buf = buf_create();
	defer_buf(buf);
	auto key = row_create_key(buf, keys, stack_at(&self->stack, keys_count), keys_count);
	stack_popn(&self->stack, keys_count);

	// open cursor
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->part  = part;

	// in case of hash index, use key only for point-lookup
	cursor->it = index_iterator(index);
	auto key_ref = key;
	if (index->config->type == INDEX_HASH && keys_count != keys->list_count)
		key_ref = NULL;
	iterator_open(cursor->it, key_ref);

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
	auto table = table_mgr_find(&self->db->table_mgr, &name_schema, &name_table, true);
	Part* part;
	if (table->config->shared)
		part = container_of(table->part_list.list.next, Part, link);
	else
		part = self->part;
	assert(part);

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
	Part* part;
	if (table->config->shared)
		part = container_of(table->part_list.list.next, Part, link);
	else
		part = self->part;
	assert(part);

	// prepare cursor and primary index iterator for related partition
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->part  = part;
	cursor->it    = index_iterator(part_primary(cursor->part));
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table*]

	// find related table partition
	auto table   = (Table*)op->a;
	auto part    = self->part;
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
