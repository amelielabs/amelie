
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
#include <amelie_value.h>
#include <amelie_store.h>
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
#include <amelie_executor.h>
#include <amelie_vm.h>

hot static inline Row*
value_row_key(Keys* self, Stack* stack)
{
	int size = 0;
	list_foreach(&self->list)
	{
		auto key = list_at(Key, link);
		switch (key->column->type) {
		case TYPE_INT8:
		case TYPE_INT16:
		case TYPE_INT32:
		case TYPE_INT64:
		case TYPE_TIMESTAMP:
			size += key->column->type_size;
			break;
		case TYPE_TEXT:
		{
			auto ref = stack_at(stack, self->list_count - key->order);
			size += json_size_string(str_size(&ref->string));
			break;
		}
		}
	}
	auto     columns_count = self->columns->list_count;
	auto     row   = row_allocate(columns_count, size);
	uint8_t* pos   = row_data(row, columns_count);
	auto     order = 0;
	list_foreach(&self->columns->list)
	{
		auto column = list_at(Column, link);
		if (! column->key)
		{
			row_set_null(row, column->order);
			continue;
		}
		auto ref = stack_at(stack, self->list_count - order);
		order++;

		// set index to pos
		row_set(row, column->order, pos - (uint8_t*)row);
		switch (column->type) {
		case TYPE_INT8:
			*(int8_t*)pos = ref->integer;
			pos += sizeof(int8_t);
			break;
		case TYPE_INT16:
			*(int16_t*)pos = ref->integer;
			pos += sizeof(int16_t);
			break;
		case TYPE_INT32:
			*(int32_t*)pos = ref->integer;
			pos += sizeof(int32_t);
			break;
		case TYPE_INT64:
			*(int64_t*)pos = ref->integer;
			pos += sizeof(int64_t);
			break;
		case TYPE_TIMESTAMP:
			*(uint64_t*)pos = ref->integer;
			pos += sizeof(int64_t);
			break;
		case TYPE_TEXT:
			json_write_string(&pos, &ref->string);
			break;
		}
	}

	return row;
}

hot static inline int
value_update_prepare(Columns* self, Row* src, Stack* stack, int count)
{
	auto values = stack_at(stack, count * 2);
	auto order  = 0;
	auto size   = 0;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		Value* value = NULL;
		if (order < count && values[order * 2].integer == column->order)
		{
			value = &values[order * 2 + 1];
			order++;
		}

		if (value)
		{
			// null
			if (value->type == VALUE_NULL)
			{
				if (column->constraint.not_null)
				{
					// NOT NULL constraint
					if (unlikely(column->constraint.not_null))
						error("column %.*s: cannot be null", str_size(&column->name),
						      str_of(&column->name));
				}
				continue;
			}

			switch (column->type) {
			case TYPE_TEXT:
				size += json_size_string(str_size(&value->string));
				break;
			case TYPE_JSON:
				size += value->json_size;
				break;
			case TYPE_VECTOR:
				size += vector_size(value->vector);
				break;
			default:
				// fixed types
				size += column->type_size;
				break;
			}

			continue;
		}

		// null
		uint8_t* pos_src = row_at(src, column->order);
		if (! pos_src)
			continue;

		switch (column->type) {
		case TYPE_TEXT:
		case TYPE_JSON:
		{
			auto pos_end = pos_src;
			json_skip(&pos_end);
			size += pos_end - pos_src;
			break;
		}
		case TYPE_VECTOR:
			size += vector_size((Vector*)pos_src);
			break;
		default:
			// fixed types
			size += column->type_size;
			break;
		}
	}
	return size;
}

hot static inline Row*
value_update(Columns* self, Row* src, Stack* stack, int count)
{
	// merge source row columns data with columns on stack
	//
	// [order, value, order, value, ...]
	//
	auto     row_size = value_update_prepare(self, src, stack, count);
	auto     row      = row_allocate(self->list_count, row_size);
	uint8_t* pos      = row_data(row, self->list_count);

	auto values = stack_at(stack, count * 2);
	auto order  = 0;
	list_foreach(&self->list)
	{
		auto column = list_at(Column, link);
		Value* value = NULL;
		if (order < count && values[order * 2].integer == column->order)
		{
			value = &values[order * 2 + 1];
			order++;
		}

		if (value)
		{
			// null
			if (value->type == VALUE_NULL)
			{
				row_set_null(row, column->order);
				continue;
			}

			row_set(row, column->order, pos - (uint8_t*)row);
			switch (column->type) {
			case TYPE_BOOL:
			case TYPE_INT8:
				*(int8_t*)pos = value->integer;
				pos += sizeof(int8_t);
				break;
			case TYPE_INT16:
				*(int16_t*)pos = value->integer;
				pos += sizeof(int16_t);
				break;
			case TYPE_INT32:
				*(int32_t*)pos = value->integer;
				pos += sizeof(int32_t);
				break;
			case TYPE_INT64:
				*(int64_t*)pos = value->integer;
				pos += sizeof(int64_t);
				break;
			case TYPE_FLOAT:
				*(float*)pos = value->dbl;
				pos += sizeof(float);
				break;
			case TYPE_DOUBLE:
				*(double*)pos = value->dbl;
				pos += sizeof(double);
				break;
			case TYPE_TIMESTAMP:
				*(uint64_t*)pos = value->integer;
				pos += sizeof(int64_t);
				break;
			case TYPE_INTERVAL:
				*(Interval*)pos = value->interval;
				pos += sizeof(Interval);
				break;
			case TYPE_TEXT:
				json_write_string(&pos, &value->string);
				break;
			case TYPE_JSON:
				memcpy(pos, value->json, value->json_size);
				pos += value->json_size;
				break;
			case TYPE_VECTOR:
				memcpy(pos, value->vector, vector_size(value->vector));
				pos += vector_size(value->vector);
				break;
			}

			continue;
		}

		// null
		uint8_t* pos_src = row_at(src, column->order);
		if (! pos_src)
		{
			row_set_null(row, column->order);
			continue;
		}

		row_set(row, column->order, pos - (uint8_t*)row);
		switch (column->type) {
		case TYPE_TEXT:
		case TYPE_JSON:
		{
			auto pos_end = pos_src;
			json_skip(&pos_end);
			memcpy(pos, pos_src, pos_end - pos_src);
			pos += pos_end - pos_src;
			break;
		}
		case TYPE_VECTOR:
			memcpy(pos, pos_src, vector_size((Vector*)pos_src));
			pos += vector_size((Vector*)pos_src);
			break;
		default:
			// fixed column types
			memcpy(pos, pos_src, column->type_size);
			pos += column->type_size;
			break;
		}
	}

	return row;
}

#if 0
hot static inline uint32_t
value_row_generate(Columns*  columns,
                   Keys*     keys,
                   Buf*      data,
                   uint8_t** pos,
                   Value*    values)
{
	// [
	if (unlikely(! data_is_array(*pos)))
		error("row type expected to be array");
	data_read_array(pos);
	encode_array(data);

	uint32_t hash = 0;
	int order = 0;
	list_foreach(&columns->list)
	{
		auto column = list_at(Column, link);
		if (unlikely(data_is_array_end(*pos)))
			error("row has incorrect number of columns");

		// skip column data
		auto at = *pos;
		data_skip(pos);

		// use generated or existing column
		int offset = buf_size(data);
		if (! str_empty(&column->constraint.as_stored))
		{
			value_write(&values[order], data);
			order++;
		} else
		{
			// copy
			buf_write(data, at, *pos - at);
		}

		// validate column data type
		auto pos_new = data->start + offset;
		if (data_is_null(pos_new))
		{
			// NOT NULL constraint
			if (unlikely(column->constraint.not_null))
				error("column %.*s: cannot be null",
				      str_size(&column->name),
				      str_of(&column->name));
		} else
		if (unlikely(! type_validate(column->type, pos_new))) {
			error("column %.*s: does not match data type %s",
			      str_size(&column->name),
			      str_of(&column->name),
			      type_of(column->type));
		}

		// indexate keys per column
		if (column->key)
		{
			list_foreach(&keys->list)
			{
				auto key = list_at(Key, link);
				if (key->column != column)
					continue;

				// find key path and validate data type
				uint8_t* pos_key = pos_new;
				key_find(key, &pos_key);

				// hash key
				hash = key_hash(hash, pos_key);
			}
		}
	}

	// ]
	if (unlikely(! data_read_array_end(pos)))
		error("row has incorrect number of columns");
	encode_array_end(data);
	return hash;
}
#endif

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

hot void
csend_generated(Vm* self, Op* op)
{
	(void)self;
	(void)op;
	// TODO
#if 0
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
	json_read_array(&data);
	while (! json_read_array_end(&data))
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
#endif
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

hot void
cupdate(Vm* self, Op* op)
{
	// [cursor, order/value count]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto row_src = iterator_at(cursor->it);
	auto row = value_update(table_columns(cursor->table), row_src, &self->stack, op->b);
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
