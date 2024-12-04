
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_vm.h>

hot void
csend(Vm* self, Op* op)
{
	// [stmt, start, table, values*]
	auto dtr    = self->dtr;
	auto table  = (Table*)op->c;
	auto values = (Set*)op->d;

	// redistribute rows between nodes
	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	ReqList list;
	req_list_init(&list);
	for (auto order = 0; order < values->count_rows; order++)
	{
		auto row_meta = set_meta(values, order);

		// map to node
		auto route = part_map_get(&table->part_list.map, row_meta->hash);
		auto req   = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_EXECUTE);
			req->start = op->b;
			req->arg_values = values;
			req->route = route;
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
	req->start = op->b;
	req->route = route;
	req_list_add(&list, req);

	executor_send(self->executor, dtr, op->a, &list);
}

hot void
csend_generated(Vm* self, Op* op)
{
	// [stmt, start, table, values*]
	auto dtr     = self->dtr;
	auto table   = (Table*)op->c;
	auto values  = (Set*)op->d;
	auto columns = table_columns(table);
	auto keys    = table_keys(table);

	// apply generated columns and redistribute rows between nodes
	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	auto count = values->count_rows * columns->generated_columns;
	auto pos = stack_at(&self->stack, count);

	ReqList list;
	req_list_init(&list);
	for (auto order = 0; order < values->count_rows; order++)
	{
		// apply generated columns and recalculate row meta
		auto row_meta = set_meta(values, order);
		row_meta->hash = 0;
		row_meta->row_size =
			row_update_values(columns, keys, set_row(values, order),
			                  pos, &row_meta->hash);
		pos += columns->generated_columns;

		// map to node
		auto route = part_map_get(&table->part_list.map, row_meta->hash);
		auto req   = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_EXECUTE);
			req->start = op->b;
			req->arg_values = values;
			req->route = route;
			req_list_add(&list, req);
			map[route->order] = req;
		}

		// write u32 row position to the req->arg
		buf_write(&req->arg, &order, sizeof(order));
	}

	executor_send(self->executor, dtr, op->a, &list);

	// todo: optimize
	stack_popn(&self->stack, count);
}

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

	// create merge object
	auto merge = merge_create(distinct, limit, offset);
	value_set_merge(reg_at(&self->r, op->a), &merge->store);

	// add requests results to the merge
	auto stmt = dispatch_stmt(&self->dtr->dispatch, op->d);
	list_foreach(&stmt->req_list.list)
	{
		auto req = list_at(Req, link);
		auto value = &req->result;
		if (value->type == TYPE_SET)
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
		assert(value->type == TYPE_SET);
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
ctable_open(Vm* self, Op* op)
{
	// [cursor, name_offset, _where, is_point_lookup]
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
	auto part  = part_list_match(&table->part_list, self->node);
	auto index = part_find(part, &name_index, true);
	auto keys  = index_keys(index);

	// create cursor key
	auto key = row_create_key(keys, stack_at(&self->stack, keys->list_count));
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
ctable_prepare(Vm* self, Op* op)
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
	auto table   = (Table*)op->a;
	auto part    = part_list_match(&table->part_list, self->node);
	auto columns = table_columns(table);

	// insert
	auto list       = buf_u32(self->code_arg);
	int  list_count = buf_size(self->code_arg) / sizeof(uint32_t);
	for (int i = 0; i < list_count; i++)
	{
		auto row_meta = set_meta(self->code_values, list[i]);
		auto row = row_create(columns, set_row(self->code_values, list[i]),
		                      row_meta->row_size);
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
		cursor->ref_count = buf_size(self->code_arg) / sizeof(uint32_t);

	// insert or upsert
	auto list = buf_u32(self->code_arg);
	while (cursor->ref_pos < cursor->ref_count)
	{
		// set cursor ref pointer to the current insert row
		cursor->ref = list[cursor->ref_pos];
		cursor->ref_pos++;

		auto row_meta = set_meta(self->code_values, cursor->ref);
		auto row = row_create(columns, set_row(self->code_values, cursor->ref),
		                      row_meta->row_size);

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
cupdate(Vm* self, Op* op)
{
	// [cursor, order/value count]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto row_src = iterator_at(cursor->it);
	auto row_values = stack_at(&self->stack, op->b * 2);
	auto row = row_update(row_src, table_columns(cursor->table), row_values, op->b);
	part_update(cursor->part, self->tr, cursor->it, row);
	stack_popn(&self->stack, op->b * 2);
}

hot void
cdelete(Vm* self, Op* op)
{
	// [cursor]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	part_delete(cursor->part, self->tr, cursor->it);
}
