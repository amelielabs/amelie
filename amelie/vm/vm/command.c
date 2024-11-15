
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
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_executor.h>
#include <amelie_vm.h>

hot void
csend(Vm* self, Op* op)
{
	// [stmt, start, table, rows_offset]
	auto dtr   = self->dtr;
	auto table = (Table*)op->c;

	// redistribute rows between nodes
	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	ReqList list;
	req_list_init(&list);

	// get the number of inserted rows
	int count = stack_at(&self->stack, 1)->integer;
	stack_pop(&self->stack);
	uint32_t order      = op->d;
	uint32_t order_last = op->d + count;
	for (; order < order_last; order++)
	{
		auto row = row_data_at(self->code_data_row, order);

		// map to node
		auto route = part_map_get(&table->part_list.map, row->hash);
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_EXECUTE);
			req->start    = op->b;
			req->arg_data = self->code_data_row;
			req->route    = route;
			req_list_add(&list, req);
			map[route->order] = req;
		}

		// write u32 row position to the req->arg
		buf_write(&req->arg, &order, sizeof(order));
	}

	executor_send(self->executor, dtr, op->a, &list);
}

hot void
csend_hash(Vm* self, Op* op)
{
	// [stmt, start, table, hash]
	auto dtr   = self->dtr;
	auto table = (Table*)op->c;

	ReqList list;
	req_list_init(&list);

	// shard by precomputed key hash
	auto route = part_map_get(&table->part_list.map, op->d);
	auto req = req_create(&dtr->req_cache, REQ_EXECUTE);
	req->start    = op->b;
	req->arg_data = NULL;
	req->route    = route;
	req_list_add(&list, req);

	executor_send(self->executor, dtr, op->a, &list);
}

#if 0
hot void
csend_generated(Vm* self, Op* op)
{
	// [stmt, start, table, rows_offset]
	auto dtr     = self->dtr;
	auto start   = op->b;
	auto table   = (Table*)op->c;
	auto columns = table_columns(table);
	auto keys    = table_keys(table);

	ReqList list;
	req_list_init(&list);

	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	// get the number of inserted rows
	int rows = stack_at(&self->stack, 1)->integer;
	int rows_values = rows * columns->generated_columns;
	stack_popn(&self->stack, 1);
	auto values = stack_at(&self->stack, rows_values);

	// create new rows by merging inserted rows with generated
	// columns created on the stack for each row
	//
	// [[], ...]
	auto data = code_data_at(self->code_data, op->d);
	data_read_array(&data);
	while (! data_read_array_end(&data))
	{
		// generate new row and hash keys
		uint32_t offset = buf_size(&self->code_data->data_generated);
		uint32_t hash = value_row_generate(columns,
		                                   keys,
		                                   &self->code_data->data_generated,
		                                   &data,
		                                   values);
		// map to node
		auto route = part_map_get(&table->part_list.map, hash);
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_EXECUTE);
			req->start = start;
			req->arg_buf = &self->code_data->data_generated;
			req->route = route;
			req_list_add(&list, req);
			map[route->order] = req;
		}

		// write u32 offset to req->arg
		buf_write(&req->arg, &offset, sizeof(offset));

		values += columns->generated_columns;
	}

	executor_send(self->executor, dtr, op->a, &list);

	stack_popn(&self->stack, rows_values);
}
#endif

hot void
csend_first(Vm* self, Op* op)
{
	// [stmt, start]
	ReqList list;
	req_list_init(&list);

	// send to the first node
	auto dtr = self->dtr;
	auto req = req_create(&dtr->req_cache, REQ_EXECUTE);
	req->route = router_first(dtr->router);
	req->start = op->b;
	req_list_add(&list, req);

	executor_send(self->executor, dtr, op->a, &list);
}

hot void
csend_all(Vm* self, Op* op)
{
	// [stmt, start, table]
	auto table = (Table*)op->c;

	ReqList list;
	req_list_init(&list);

	// send to all table nodes
	auto dtr = self->dtr;
	auto map = &table->part_list.map;
	for (auto i = 0; i < map->list_count; i++)
	{
		auto route = map->list[i];
		auto req = req_create(&dtr->req_cache, REQ_EXECUTE);
		req->route = route;
		req->start = op->b;
		req_list_add(&list, req);
	}

	executor_send(self->executor, dtr, op->a, &list);
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

	auto stmt = dispatch_stmt(&self->dtr->dispatch, op->b);
	auto req  = container_of(list_first(&stmt->req_list.list), Req, link);
	value_move(reg_at(&self->r, op->a), &req->result);
}

hot void
cmerge(Vm* self, Op* op)
{
	// [merge, set, limit, offset]

	// distinct
	bool distinct = stack_at(&self->stack, 1)->integer;
	stack_popn(&self->stack, 1);

	// limit
	int64_t limit = INT64_MAX;
	if (op->c != -1)
	{
		if (unlikely(reg_at(&self->r, op->c)->type != VALUE_INT))
			error("LIMIT: integer type expected");
		limit = reg_at(&self->r, op->c)->integer;
		if (unlikely(limit < 0))
			error("LIMIT: positive integer value expected");
	}

	// offset
	int64_t offset = 0;
	if (op->d != -1)
	{
		if (unlikely(reg_at(&self->r, op->d)->type != VALUE_INT))
			error("OFFSET: integer type expected");
		offset = reg_at(&self->r, op->d)->integer;
		if (unlikely(offset < 0))
			error("OFFSET: positive integer value expected");
	}

	// create merge object
	auto merge = merge_create(distinct, limit, offset);
	value_set_merge(reg_at(&self->r, op->a), &merge->store);

	// add set
	auto value = reg_at(&self->r, op->b);
	merge_add(merge, (Set*)value->store);
	value_reset(value);
}

hot void
cmerge_recv(Vm* self, Op* op)
{
	// [merge, limit, offset, stmt]

	// distinct
	bool distinct = stack_at(&self->stack, 1)->integer;
	stack_popn(&self->stack, 1);

	// limit
	int64_t limit = INT64_MAX;
	if (op->b != -1)
	{
		if (unlikely(reg_at(&self->r, op->b)->type != VALUE_INT))
			error("LIMIT: integer type expected");
		limit = reg_at(&self->r, op->b)->integer;
		if (unlikely(limit < 0))
			error("LIMIT: positive integer value expected");
	}

	// offset
	int64_t offset = 0;
	if (op->c != -1)
	{
		if (unlikely(reg_at(&self->r, op->c)->type != VALUE_INT))
			error("OFFSET: integer type expected");
		offset = reg_at(&self->r, op->c)->integer;
		if (unlikely(offset < 0))
			error("OFFSET: positive integer value expected");
	}

	// create merge object
	auto merge = merge_create(distinct, limit, offset);
	value_set_merge(reg_at(&self->r, op->a), &merge->store);

	// add requests results to the merge
	auto stmt = dispatch_stmt(&self->dtr->dispatch, op->d);
	list_foreach(&stmt->req_list.list)
	{
		auto req = list_at(Req, link);
		auto value = &req->result;
		if (value->type == VALUE_SET)
		{
			merge_add(merge, (Set*)value->store);
			value_reset(value);
		}
	}
}

hot void
cmerge_recv_agg(Vm* self, Op* op)
{
	// [set, stmt, aggs]
	auto stmt = dispatch_stmt(&self->dtr->dispatch, op->b);
	if (unlikely(! stmt->req_list.list_count))
		error("unexpected group list return");
	
	auto   list_count = stmt->req_list.list_count;
	Value* list[list_count];
	
	// collect a list of returned sets
	int pos = 0;
	list_foreach(&stmt->req_list.list)
	{
		auto req = list_at(Req, link);
		auto value = &req->result;
		assert(value->type == VALUE_SET);
		list[pos++] = value;
	}

	// merge hash sets aggregates into the first set and
	// free other sets
	agg_merge(list, list_count, (int*)code_data_at(self->code_data, op->c));

	// return merged set
	auto value = list[0];
	value_set_set(reg_at(&self->r, op->a), value->store);
	value_reset(value);
}

hot Op*
ccursor_open(Vm* self, Op* op)
{
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// read names
	uint8_t* pos = code_data_at(self->code_data, op->b);
	Str name_schema;
	Str name_table;
	Str name_index;
	data_read_string(&pos, &name_schema);
	data_read_string(&pos, &name_table);
	data_read_string(&pos, &name_index);

	// find table, partition and index
	auto table = table_mgr_find(&self->db->table_mgr, &name_schema, &name_table, true);
	auto part  = part_list_match(&table->part_list, self->node);
	auto index = part_find(part, &name_index, true);
	auto keys  = index_keys(index);

	// create cursor key
	auto key = value_row_key(keys, &self->stack);
	guard(row_free, key);
	stack_popn(&self->stack, keys->list_count);

	// open cursor
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->part  = part;

	// in case of hash index, use key only for point-lookup
	cursor->it = index_iterator(index);
	auto key_ref = key;
	if (index->config->type == INDEX_HASH && !op->d)
		key_ref = NULL;
	iterator_open(cursor->it, key_ref);

	// jmp if has data
	if (iterator_has(cursor->it))
		return code_at(self->code, op->c);
	return ++op;
}

hot void
ccursor_prepare(Vm* self, Op* op)
{
	// [target_id, table*]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// find partition
	Table* table = (Table*)op->b;

	// prepare cursor and primary index iterator for related partition
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->part  = part_list_match(&table->part_list, self->node);
	cursor->it    = index_iterator(part_primary(cursor->part));
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table*]

	// find related table partition
	auto table = (Table*)op->a;
	auto part  = part_list_match(&table->part_list, self->node);

	// insert
	auto list       = buf_u32(self->code_arg);
	int  list_count = buf_size(self->code_arg) / sizeof(uint32_t);
	for (int i = 0; i < list_count; i++)
	{
		auto row = row_data_create(self->code_data_row, list[i]);
		part_insert(part, self->tr, false, row);
	}
}

hot Op*
cupsert(Vm* self, Op* op)
{
	// [target_id, _jmp, _jmp_returning]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// first call
	if (cursor->ref_pos == 0)
		cursor->ref_count = buf_size(self->code_arg) / sizeof(uint32_t);

	// free row from previous upsert iteration
	if (cursor->ref)
	{
		row_free(cursor->ref);
		cursor->ref = NULL;
	}

	// insert or upsert
	auto list = buf_u32(self->code_arg);
	while (cursor->ref_pos < cursor->ref_count)
	{
		auto row = row_data_create(self->code_data_row, list[cursor->ref_pos]);
		cursor->ref_pos++;

		// insert or get (open iterator in both cases)
		auto exists = part_upsert(cursor->part, self->tr, cursor->it, row);
		if (exists)
		{
			// upsert

			// set cursor ref pointer to the current insert row,
			// the row will be freed on next iteration
			cursor->ref = row;

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

#if 0
hot void
cupdate(Vm* self, Op* op)
{
	// [cursor]
	auto tr     = self->tr;
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto part   = cursor->part;
	auto it     = cursor->it;

	// update by cursor
	uint8_t* pos;
	auto value = stack_at(&self->stack, 1);
	switch (value->type) {
	case VALUE_OBJ:
	case VALUE_ARRAY:
	{
		pos = value->data;
		part_update(part, tr, it, &pos);
		break;
	}
	case VALUE_SET:
	{
		auto buf = buf_create();
		guard_buf(buf);
		auto set = (Set*)value->store;
		for (int j = 0; j < set->list_count; j++)
		{
			auto ref = &set_at(set, j)->value;
			if (likely(ref->type == VALUE_ARRAY ||
			           ref->type == VALUE_OBJ))
			{
				pos = ref->data;
			} else
			{
				buf_reset(buf);
				value_write(ref, buf);
				pos = buf->start;
			}
			part_update(part, tr, it, &pos);
		}
		break;
	}
	default:
		error("UPDATE: array, object or set expected, but got %s",
		      value_type_to_string(value->type));
		break;
	}

	stack_popn(&self->stack, 1);
}
#endif

hot void
cdelete(Vm* self, Op* op)
{
	// delete by cursor
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	part_delete(cursor->part, self->tr, cursor->it);
}
