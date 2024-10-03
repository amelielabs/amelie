
//
// amelie.
//
// Real-Time SQL Database.
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
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>

hot Op*
ccursor_open(Vm* self, Op* op)
{
	// [target_id, name_offset, _where, point_lookup]
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
	uint8_t key_data[keys->key_size];
	auto    key = (Ref*)key_data;
	value_ref(keys, key, &self->stack);
	guard(row_free, key->row);
	stack_popn(&self->stack, keys->list_count);

	// open cursor
	cursor->type  = CURSOR_TABLE;
	cursor->table = table;
	cursor->keys  = keys;
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

hot static bool
ccursor_open_value(Vm* self, int target, Value* value)
{
	auto cursor = cursor_mgr_of(&self->cursor_mgr, target);
	switch (value->type) {
	case VALUE_MAP:
	{
		cursor->type    = CURSOR_MAP;
		cursor->obj_pos = value->data;
		data_read_map(&cursor->obj_pos);
		auto end = data_is_map_end(cursor->obj_pos);
		if (! end)
		{
			cursor->ref_key = cursor->obj_pos;
			data_skip(&cursor->obj_pos);
		}
		if (end)
			return false;
		break;
	}
	case VALUE_ARRAY:
	{
		cursor->type    = CURSOR_ARRAY;
		cursor->obj_pos = value->data;
		data_read_array(&cursor->obj_pos);
		auto end = data_is_array_end(cursor->obj_pos);
		if (end)
			return false;
		break;
	}
	case VALUE_SET:
	{
		cursor->type = CURSOR_SET;
		auto set = (Set*)value->obj;
		set_iterator_open(&cursor->set_it, set);
		if (! set_iterator_has(&cursor->set_it))
			return false;
		break;
	}
	case VALUE_MERGE:
	{
		cursor->type = CURSOR_MERGE;
		auto merge = (Merge*)value->obj;
		merge_iterator_open(&cursor->merge_it, merge);
		if (! merge_iterator_has(&cursor->merge_it))
			return false;
		break;
	}
	case VALUE_GROUP:
	{
		cursor->type  = CURSOR_GROUP;
		cursor->group = (Group*)value->obj;
		if (cursor->group->ht.count == 0)
			return false;
		cursor->group_pos = group_next(cursor->group, -1);
		break;
	}
	default:
		error("FROM: array, map or data type expected, but got %s",
		      value_type_to_string(value->type));
		break;
	}

	// jmp on success
	return true;
}

hot Op*
ccursor_open_expr(Vm* self, Op* op)
{
	// [target_id, rexpr, _eof]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	cursor->r = op->b;
	auto expr = reg_at(&self->r, op->b);
	if (ccursor_open_value(self, op->a, expr))
		return code_at(self->code, op->c);
	return ++op;
}

hot Op*
ccursor_open_cte(Vm* self, Op* op)
{
	// [target_id, cte, _eof]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	cursor->cte = true;
	auto expr = result_at(self->cte, op->b);
	if (ccursor_open_value(self, op->a, expr))
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
	cursor->keys  = table_keys(table);
	cursor->part  = part_list_match(&table->part_list, self->node);
	cursor->it    = index_iterator(part_primary(cursor->part));
}

hot void
ccursor_close(Vm* self, Op* op)
{
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	switch (cursor->type) {
	case CURSOR_TABLE:
		break;
	case CURSOR_ARRAY:
	case CURSOR_MAP:
	case CURSOR_SET:
	case CURSOR_MERGE:
	case CURSOR_GROUP:
	{
		if (cursor->cte)
			break;
		value_free(reg_at(&self->r, cursor->r));
		break;
	}
	default:
		assert(0);
		break;
	}
	cursor_reset(cursor);
}

hot Op*
ccursor_next(Vm* self, Op* op)
{
	// [target_id, _on_success]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	switch (cursor->type) {
	case CURSOR_TABLE:
	{
		// next
		iterator_next(cursor->it);

		// check for eof
		if (! iterator_has(cursor->it))
			return ++op;
		break;
	}
	case CURSOR_ARRAY:
	{
		// array
		data_skip(&cursor->obj_pos);
		if (data_read_array_end(&cursor->obj_pos))
			return ++op;
		break;
	}
	case CURSOR_MAP:
	{
		// skip previous value
		data_skip(&cursor->obj_pos);
		if (data_read_map_end(&cursor->obj_pos))
			return ++op;
		// set and skip key
		cursor->ref_key = cursor->obj_pos;
		data_skip(&cursor->obj_pos);
		break;
	}
	case CURSOR_SET:
	{
		set_iterator_next(&cursor->set_it);
		if (! set_iterator_has(&cursor->set_it))
			return ++op;
		break;
	}
	case CURSOR_MERGE:
	{
		merge_iterator_next(&cursor->merge_it);
		if (! merge_iterator_has(&cursor->merge_it))
			return ++op;
		break;
	}
	case CURSOR_GROUP:
	{
		int next;
		next = group_next(cursor->group, cursor->group_pos);
		if (next == -1)
			return ++op;
		cursor->group_pos = next;
		break;
	}
	default:
		assert(0);
		break;
	}

	// jmp on success
	return code_at(self->code, op->b);
}

hot void
ccursor_read(Vm* self, Op* op)
{
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->b);
	auto a = reg_at(&self->r, op->a);
	switch (cursor->type) {
	case CURSOR_TABLE:
	{
		if (unlikely(! iterator_has(cursor->it)))
		{
			auto config = cursor->table->config;
			error("%.*s.%.*s.*: not in active scan",
			      str_size(&config->schema), str_of(&config->schema),
			      str_size(&config->name), str_of(&config->name));
		}
		auto current = iterator_at(cursor->it);
		assert(current != NULL);
		value_set_array(a, row_data(current), row_size(current), NULL);
		break;
	}
	case CURSOR_ARRAY:
	case CURSOR_MAP:
	{
		value_read(a, cursor->obj_pos, NULL);
		break;
	}
	case CURSOR_SET:
	{
		auto ref = &set_iterator_at(&cursor->set_it)->value;
		value_copy_ref(a, ref);
		break;
	}
	case CURSOR_MERGE:
	{
		auto ref = &merge_iterator_at(&cursor->merge_it)->value;
		value_copy_ref(a, ref);
		break;
	}
	case CURSOR_GROUP:
	{
		auto node = group_at(cursor->group, cursor->group_pos);
		group_read(cursor->group, node, a);
		break;
	}
	default:
		error("*: not in active scan");
		break;
	}
}

hot void
ccursor_idx(Vm* self, Op* op)
{
	// [result, cursor, column, string_offset]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->b);

	Buf*     data_buf = NULL;
	uint8_t* data = NULL;
	switch (cursor->type) {
	case CURSOR_TABLE:
	{
		if (unlikely(! iterator_has(cursor->it)))
		{
			auto config = cursor->table->config;
			error("%.*s.%.*s.*: not in active scan",
			      str_size(&config->schema), str_of(&config->schema),
			      str_size(&config->name), str_of(&config->name));
		}
		auto current = iterator_at(cursor->it);
		assert(current != NULL);
		data     = row_data(current);
		data_buf = NULL;
		break;
	}
	case CURSOR_ARRAY:
	case CURSOR_MAP:
		data_buf = reg_at(&self->r, cursor->r)->buf;
		data     = cursor->obj_pos;
		break;
	case CURSOR_SET:
	{
		auto value = &set_iterator_at(&cursor->set_it)->value;
		if (value->type != VALUE_MAP && value->type != VALUE_ARRAY)
			error("cursor: object expected to be array or map, but got %s",
			      value_type_to_string(value->type));
		data     = value->data;
		data_buf = value->buf;
		break;
	}
	case CURSOR_MERGE:
	{
		auto value = &merge_iterator_at(&cursor->merge_it)->value;
		if (value->type != VALUE_MAP && value->type != VALUE_ARRAY)
			error("cursor: object expected to be array or map, but got %s",
			      value_type_to_string(value->type));
		data     = value->data;
		data_buf = value->buf;
		break;
	}
	default:
		// should not happen
		error("cursor: cursor is not active");
		break;
	}

	if (cursor->cte)
		data_buf = NULL;

	// access by column path
	if (op->c != -1)
	{
		int column_order = op->c;
		if (unlikely(! data_is_array(data)))
			error("[]: array expected, but got %s",
			       type_to_string(*data));

		if (! array_find(&data, column_order))
			error("[]: index %d is out of bounds", column_order);
	}

	// match name in the object
	if (op->d != -1)
	{
		Str name;
		code_data_at_string(self->code_data, op->d, &name);
		if (unlikely(! data_is_map(data)))
		{
			if (cursor->type == CURSOR_TABLE && op->c == -1)
				error("column %.*s: does not exists", str_size(&name), str_of(&name));
			else
				error("column %.*s: row or map expected, but got %s",
				      str_size(&name), str_of(&name),
				      type_to_string(*data));
		}

		if (! map_find_path(&data, &name))
		{
			if (cursor->type == CURSOR_TABLE)
			{
				auto column = columns_of(table_columns(cursor->table), op->c);
				error("column %.*s.%.*s: path not found",
				      str_size(&column->name), str_of(&column->name),
				      str_size(&name), str_of(&name));
			} else {
				error("%.*s: path not found", str_size(&name),
				      str_of(&name));
			}
		}
	}

	value_read_ref(reg_at(&self->r, op->a), data, data_buf);
}

hot void
ccall(Vm* self, Op* op)
{
	// [result, function, argc, call_id]

	// prepare call arguments
	int    argc = op->c;
	Value* argv[argc];
	for (int i = 0; i < argc; i++)
		argv[i] = stack_at(&self->stack, argc - i);

	// get function execution context, if any
	void** context = NULL;
	if (op->d != -1)
		context = call_mgr_at(&self->call_mgr, op->d);

	// call the function
	auto func = (Function*)op->b;
	Call call =
	{
		.argc     = argc,
		.argv     = argv,
		.result   = reg_at(&self->r, op->a),
		.vm       = self,
		.type     = CALL_EXECUTE,
		.function = func,
		.context  = context
	};
	func->main(&call);

	stack_popn(&self->stack, op->c);
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table_ptr]
	
	// find partition
	auto table = (Table*)op->a;
	auto part  = part_list_match(&table->part_list, self->node);

	// insert
	auto list       = buf_u32(self->code_arg);
	int  list_count = buf_size(self->code_arg) / sizeof(uint32_t);
	for (int i = 0; i < list_count; i++)
	{
		uint8_t* pos = code_data_at(self->code_data, list[i]);
		if (unlikely(! data_is_array(pos)))
			error("INSERT/REPLACE: array expected, but got %s",
			      type_to_string(*pos));
		part_insert(part, self->tr, false, &pos);
	}
}

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
	case VALUE_MAP:
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
		auto set = (Set*)value->obj;
		for (int j = 0; j < set->list_count; j++)
		{
			auto ref = &set_at(set, j)->value;
			if (likely(ref->type == VALUE_ARRAY ||
			           ref->type == VALUE_MAP))
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
		error("UPDATE: array, map or set expected, but got %s",
		      value_type_to_string(value->type));
		break;
	}

	stack_popn(&self->stack, 1);
}

hot void
cdelete(Vm* self, Op* op)
{
	// delete by cursor
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	part_delete(cursor->part, self->tr, cursor->it);
}

hot Op*
cupsert(Vm* self, Op* op)
{
	// [target_id, _jmp, _jmp_returning]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// first call
	if (cursor->ref_pos == 0)
		cursor->ref_count = buf_size(self->code_arg) / sizeof(uint32_t);

	// insert or upsert
	auto list = buf_u32(self->code_arg);
	while (cursor->ref_pos < cursor->ref_count)
	{
		uint8_t* pos = code_data_at(self->code_data, list[cursor->ref_pos]);
		if (unlikely(! data_is_array(pos)))
			error("UPSERT: array expected, but got %s",
			      type_to_string(*pos));

		// set cursor ref pointer to the current insert row data
		cursor->ref = pos;
		cursor->ref_pos++;

		// insert or get (open iterator in both cases)
		auto exists = part_upsert(cursor->part, self->tr, cursor->it, &pos);
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
	cursor->ref = NULL;
	return ++op;
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
	auto merge = merge_create();
	value_set_merge(reg_at(&self->r, op->a), &merge->obj);

	// add set
	auto value = reg_at(&self->r, op->b);
	merge_add(merge, (Set*)value->obj);
	value_reset(value);

	// prepare merge and apply offset
	merge_open(merge, distinct, limit, offset);
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
	auto merge = merge_create();
	value_set_merge(reg_at(&self->r, op->a), &merge->obj);

	// add requests results to the merge
	auto stmt = dispatch_stmt(&self->dtr->dispatch, op->d);
	list_foreach(&stmt->req_list.list)
	{
		auto req = list_at(Req, link);
		auto value = &req->result;
		if (value->type == VALUE_SET)
		{
			merge_add(merge, (Set*)value->obj);
			value_reset(value);
		}
	}

	// prepare merge and apply offset
	merge_open(merge, distinct, limit, offset);
}

hot void
cgroup_merge_recv(Vm* self, Op* op)
{
	// [group, stmt]
	auto stmt = dispatch_stmt(&self->dtr->dispatch, op->b);
	if (unlikely(! stmt->req_list.list_count))
		error("unexpected group list return");
	
	auto   list_count = stmt->req_list.list_count;
	Value* list[list_count];
	
	// collect a list of returned groups
	int pos = 0;
	list_foreach(&stmt->req_list.list)
	{
		auto req = list_at(Req, link);
		auto value = &req->result;
		assert(value->type == VALUE_GROUP);
		list[pos++] = value;
	}

	// merge aggregates into the first group
	group_merge(list, list_count);

	// free group objects
	for (int i = 1; i < list_count; i++)
		value_free(list[i]);

	// return merged group
	auto value = list[0];
	value_set_group(reg_at(&self->r, op->a), value->obj);
	value_reset(value);
}

hot void
csend_hash(Vm* self, Op* op)
{
	// [stmt, start, table, hash]
	auto dtr   = self->dtr;
	auto start = op->b;
	auto table = (Table*)op->c;

	ReqList list;
	req_list_init(&list);

	// shard by precomputed key hash
	auto route = part_map_get(&table->part_list.map, op->d);
	auto req = req_create(&dtr->req_cache);
	req->start = start;
	req->route = route;
	req_list_add(&list, req);

	executor_send(self->executor, dtr, op->a, &list);
}

hot void
csend(Vm* self, Op* op)
{
	// [stmt, start, table, offset]
	auto dtr   = self->dtr;
	auto start = op->b;
	auto table = (Table*)op->c;
	auto keys  = table_keys(table);

	// redistribute rows between nodes
	ReqList list;
	req_list_init(&list);

	auto data_start = code_data_at(self->code_data, 0);
	auto data       = code_data_at(self->code_data, op->d);

	Req* map[dtr->set.set_size];
	memset(map, 0, sizeof(map));

	data_read_array(&data);
	while (! data_read_array_end(&data))
	{
		// hash row keys
		uint32_t offset = data - data_start;
		uint32_t hash = 0;
		row_create_hash(keys, &hash, &data);

		// map to node
		auto route = part_map_get(&table->part_list.map, hash);
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache);
			req->start = start;
			req->route = route;
			req_list_add(&list, req);
			map[route->order] = req;
		}

		// write u32 offset to req->arg
		buf_write(&req->arg, &offset, sizeof(offset));
	}

	executor_send(self->executor, dtr, op->a, &list);
}

hot void
csend_first(Vm* self, Op* op)
{
	// [stmt, start]
	ReqList list;
	req_list_init(&list);

	// send to the first node
	auto dtr = self->dtr;
	auto req = req_create(&dtr->req_cache);
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
		auto req = req_create(&dtr->req_cache);
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
