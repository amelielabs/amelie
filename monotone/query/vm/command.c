
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_index.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>
#include <monotone_vm.h>

hot Op*
ccursor_open(Vm* self, Op* op)
{
	// [target_id, name_offset, _where]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// read names
	uint8_t* pos = code_data_at(self->code_data, op->b);
	Str name_schema;
	Str name_table;
	Str name_index;
	data_read_string(&pos, &name_schema);
	data_read_string(&pos, &name_table);
	data_read_string(&pos, &name_index);

	// find table, index and storage per shard
	auto table   = table_mgr_find(&self->db->table_mgr, &name_schema, &name_table, true);
	auto def     = table_find_index_def(table, &name_index, true);
	auto storage = storage_mgr_find(&table->storage_mgr, self->shard);

	// create cursor key
	auto key = value_row_key(def, &self->stack);
	guard(row_guard, row_free, key);
	stack_popn(&self->stack, def->key_count);

	// open cursor
	cursor->type    = CURSOR_TABLE;
	cursor->table   = table;
	cursor->def     = def;
	cursor->storage = storage;
	storage_iterator_open(&cursor->it, storage, &name_index, key);

	// jmp if has data
	if (storage_iterator_has(&cursor->it))
		return code_at(self->code, op->c);
	return ++op;
}

hot Op*
ccursor_open_expr(Vm* self, Op* op)
{
	// [target_id, rexpr, _eof]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	cursor->r = op->b;

	auto expr = reg_at(&self->r, op->b);
	if (expr->type == VALUE_DATA)
	{
		if (data_is_array(expr->data))
		{
			cursor->type     = CURSOR_ARRAY;
			cursor->obj_pos  = 0;
			cursor->obj_data = expr->data;
			data_read_array(&cursor->obj_data, &cursor->obj_count);
		} else
		if (data_is_map(expr->data))
		{
			cursor->type     = CURSOR_MAP;
			cursor->obj_pos  = 0;
			cursor->obj_data = expr->data;
			data_read_map(&cursor->obj_data, &cursor->obj_count);
			if (cursor->obj_count > 0)
			{
				cursor->ref_key = cursor->obj_data;
				data_skip(&cursor->obj_data);
			}

		} else {
			error("FROM: array, map or data type expected");
		}
		if (cursor->obj_count == 0)
			return ++op;
	} else
	if (expr->type == VALUE_SET)
	{
		cursor->type = CURSOR_SET;
		auto set = (Set*)expr->obj;
		set_iterator_open(&cursor->set_it, set);
		if (! set_iterator_has(&cursor->set_it))
			return ++op;
	} else
	if (expr->type == VALUE_MERGE)
	{
		cursor->type  = CURSOR_MERGE;
		cursor->merge = (Merge*)expr->obj;
		if (! merge_has(cursor->merge))
			return ++op;
	} else
	if (expr->type == VALUE_GROUP)
	{
		cursor->type  = CURSOR_GROUP;
		cursor->group = (Group*)expr->obj;
		if (cursor->group->ht.count == 0)
			return ++op;
		cursor->group_pos = group_next(cursor->group, -1);
	} else
	{
		error("FROM: array, map or data type expected");
	}

	// jmp on success
	return code_at(self->code, op->c);
}

hot void
ccursor_prepare(Vm* self, Op* op)
{
	// [target_id, table*]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// find storage
	Table* table = (Table*)op->b;

	// prepare cursor
	cursor->type    = CURSOR_TABLE;
	cursor->table   = table;
	cursor->def     = table_def(table);
	cursor->storage = storage_mgr_find(&table->storage_mgr, self->shard);
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
		storage_iterator_next(&cursor->it);

		// check for eof
		if (! storage_iterator_has(&cursor->it))
			return ++op;
		break;
	}
	case CURSOR_ARRAY:
	{
		// array
		cursor->obj_pos++;
		if (cursor->obj_pos >= cursor->obj_count)
			return ++op;
		data_skip(&cursor->obj_data);
		break;
	}
	case CURSOR_MAP:
	{
		// map
		cursor->obj_pos++;
		if (cursor->obj_pos >= cursor->obj_count)
			return ++op;
		// skip previous value
		data_skip(&cursor->obj_data);
		// set and skip key
		cursor->ref_key = cursor->obj_data;
		data_skip(&cursor->obj_data);
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
		merge_next(cursor->merge);
		if (! merge_has(cursor->merge))
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
		if (unlikely(! storage_iterator_has(&cursor->it)))
			error("*: not in active aggregation");
		auto current = storage_iterator_at(&cursor->it);
		auto def = cursor->def;
		assert(current != NULL);
		value_set_data(a, row_data(current, def), row_data_size(current, def), NULL);
		break;
	}
	case CURSOR_ARRAY:
	case CURSOR_MAP:
	{
		value_read(a, cursor->obj_data, NULL);
		break;
	}
	case CURSOR_SET:
	{
		auto ref = &set_iterator_at(&cursor->set_it)->value;
		value_copy(a, ref);
		break;
	}
	case CURSOR_MERGE:
	{
		auto ref = &merge_at(cursor->merge)->value;
		value_copy(a, ref);
		break;
	}
	case CURSOR_GROUP:
	{
		auto node = group_at(cursor->group, cursor->group_pos);
		group_get(cursor->group, node, a);
		break;
	}
	default:
		error("*: not in active aggregation");
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
		if (unlikely(! storage_iterator_has(&cursor->it)))
			error("*: not in active aggregation");
		auto current = storage_iterator_at(&cursor->it);
		assert(current != NULL);
		data     = row_data(current, cursor->def);
		data_buf = NULL;
		break;
	}
	case CURSOR_ARRAY:
	case CURSOR_MAP:
		data_buf = reg_at(&self->r, cursor->r)->buf;
		data     = cursor->obj_data;
		break;
	case CURSOR_SET:
	{
		auto value = &set_iterator_at(&cursor->set_it)->value;
		if (value->type != VALUE_DATA)
			error("current cursor object type is not a map");
		data     = value->data;
		data_buf = value->buf;
		break;
	}
	case CURSOR_MERGE:
	{
		auto value = &merge_at(cursor->merge)->value;
		if (value->type != VALUE_DATA)
			error("current cursor object type is not a map");
		data     = value->data;
		data_buf = value->buf;
		break;
	}
	default:
		// should not happen
		error("cursor: cursor is not active");
		break;
	}

	// access by column path
	if (op->c != -1)
	{
		int column_order = op->c;
		if (unlikely(! data_is_array(data)))
			error("current cursor object is not an array");

		if (! array_find(&data, column_order))
			error("column <%d> does not exists", column_order);
	}

	// match name in the object
	if (op->d != -1)
	{
		if (unlikely(! data_is_map(data)))
			error("current cursor object type is not a map");

		Str name;
		code_data_at_string(self->code_data, op->d, &name);
		if (! map_find_path(&data, &name))
			error("object path '%.*s' not found", str_size(&name),
			      str_of(&name));
	}

	value_read(reg_at(&self->r, op->a), data, data_buf);
}

hot void
ccall(Vm* self, Op* op)
{
	// [result, function, argc]

	// prepare call arguments
	int    argc = op->c;
	Value* argv[argc];
	for (int i = 0; i < argc; i++)
		argv[i] = stack_at(&self->stack, argc - i);

	// call an internal function
	Function* func = (Function*)op->b;
	func->main(self, func, reg_at(&self->r, op->a), argc, argv);

	stack_popn(&self->stack, op->c);
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table_ptr, data_offset, data_size, unique]
	auto trx    = self->trx;
	bool unique = op->d;

	uint8_t* data = code_data_at(self->code_data, op->b);
	uint32_t data_size = op->c;
	if (unlikely(! data_is_array(data)))
		error("INSERT/REPLACE: array expected");

	// find or create partition
	Table* table = (Table*)op->a;
	auto part = table_map(table, self->shard, data, data_size);
	assert(part);

	// insert or replace
	part_set(part, trx, unique, data, data_size);
}

hot void
cupdate(Vm* self, Op* op)
{
	// [cursor]
	auto trx           = self->trx;
	auto cursor        = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto part          = cursor->it.part;
	auto part_iterator = cursor->it.part_iterator;

	// update by cursor
	auto value  = stack_at(&self->stack, 1);
	if (likely(value->type == VALUE_DATA))
	{
		if (unlikely(!data_is_array(value->data) && !data_is_map(value->data)))
			error("UPDATE: array, map or data set expected");

		part_update(part, trx, part_iterator, value->data, value->data_size);

	} else
	if (value->type == VALUE_SET)
	{
		auto set = (Set*)value->obj;
		for (int j = 0; j < set->list_count; j++)
		{
			auto ref = &set_at(set, j)->value;
			if (likely(ref->type == VALUE_DATA))
			{
				part_update(part, trx, part_iterator, ref->data, ref->data_size);
			} else
			{
				auto buf = buf_create(0);
				value_write(ref, buf);
				part_update(part, trx, part_iterator, buf->start,
				            buf_size(buf));
				buf_free(buf);
			}
		}
	} else
	{
		error("UPDATE: array, map or data set expected");
	}

	stack_popn(&self->stack, 1);
}

hot void
cdelete(Vm* self, Op* op)
{
	// delete by cursor
	auto cursor        = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto part          = cursor->it.part;
	auto part_iterator = cursor->it.part_iterator;

	part_delete(part, self->trx, part_iterator);
}

hot Op*
cupsert(Vm* self, Op* op)
{
	// [target_id, data_offset, data_size, _jmp]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	// upsert
	uint8_t* data = code_data_at(self->code_data, op->b);
	uint32_t data_size = op->c;
	if (unlikely(! data_is_array(data)))
		error("UPSERT: array expected");

	// find or create partition
	auto part = table_map(cursor->table, self->shard, data, data_size);
	assert(part);

	// on insert
	Iterator *it = NULL;
	auto on_insert = part_upsert(part, self->trx, &it, data, data_size);
	if (on_insert)
	{
		cursor->ref = NULL;
		return ++op;
	}

	// upsert

	// open storage cursor using partition iterator
	storage_iterator_reset(&cursor->it);
	storage_iterator_open_as(&cursor->it, cursor->storage, NULL, part, it);

	// set cursor ref pointer to the current insert row data
	cursor->ref = data;

	// push next operation address
	auto jmp_op = stack_push(&self->stack);
	value_set_int(jmp_op, (intptr_t)(op + 1));

	// update
	return code_at(self->code, op->d);
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
	value->type = VALUE_NONE;

	// prepare merge and apply offset
	merge_open(merge, distinct, limit, offset);
}

hot void
cmerge_recv(Vm* self, Op* op)
{
	// [merge, stmt, limit, offset]
	int stmt = op->b;

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

	// add sets from the statement
	auto dispatch = self->dispatch;
	for (int order = 0; order < dispatch->set_size; order++)
	{
		auto req = dispatch_at_stmt(dispatch, stmt, order);
		if (! req)
			continue;
		auto value = result_at(&req->result, stmt);
		if (value->type == VALUE_SET)
		{
			merge_add(merge, (Set*)value->obj);
			value->type = VALUE_NONE;
		}
	}

	// prepare merge and apply offset
	merge_open(merge, distinct, limit, offset);
}

hot void
cgroup_merge_recv(Vm* self, Op* op)
{
	// [group, stmt]
	auto   dispatch = self->dispatch;
	int    stmt = op->b;

	int    list_count = 0;
	Value* list[dispatch->set_size];

	for (int order = 0; order < dispatch->set_size; order++)
	{
		auto req = dispatch_at_stmt(dispatch, stmt, order);
		if (! req)
			continue;
		auto value = result_at(&req->result, stmt);
		if (value->type != VALUE_GROUP)
			continue;
		list[list_count++] = value;
	}

	if (unlikely(! list_count))
		error("unexpected group list return");

	// merge aggregates into the first group
	group_merge(list, list_count);

	// free group objects
	for (int i = 1; i < list_count; i++)
		value_free(list[i]);

	// return merged group
	value_set_group(reg_at(&self->r, op->a), list[0]->obj);
	list[0]->type = VALUE_NONE;
}

hot void
csend_set(Vm* self, Op* op)
{
	// [obj]
	auto value = reg_at(&self->r, op->a);

	if (value->type == VALUE_MERGE)
	{
		auto merge = (Merge*)value->obj;
		while (merge_has(merge))
		{
			auto value = &merge_at(merge)->value;
			auto buf = msg_create(MSG_OBJECT);
			value_write(value, buf);
			msg_end(buf);
			portal_write(self->portal, buf);

			merge_next(merge);
		}

	} else
	if (value->type == VALUE_SET)
	{
		auto set = (Set*)value->obj;
		for (int pos = 0; pos < set->list_count; pos++)
		{
			auto value = &set_at(set, pos)->value;
			auto buf = msg_create(MSG_OBJECT);
			value_write(value, buf);
			msg_end(buf);
			portal_write(self->portal, buf);
		}

	} else
	{
		abort();
	}

	// set or merge with all sets
	value_free(value);
}
