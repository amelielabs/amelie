
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
#include <monotone_schema.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>

hot Op*
ccursor_open(Vm* self, Op* op)
{
	// [target_id, table_offset, index_offset, _where]
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);

	Str name_table;
	Str name_index;
	code_read_string(self->code, op->b, &name_table);
	code_read_string(self->code, op->c, &name_index);

	// find table/index by name
	auto table   = table_mgr_find(&self->db->table_mgr, &name_table, true);
	auto storage = storage_mgr_find_for(&self->db->storage_mgr, self->shard,
	                                    &table->config->id);
	auto index   = storage_find(storage, &name_index, true);

	// create cursor key
	auto schema = index_schema(index);
	auto key = value_row_key(schema, &self->stack);
	guard(row_guard, row_free, key);
	stack_popn(&self->stack, schema->key_count);

	// open cursor
	cursor->type    = CURSOR_TABLE;
	cursor->table   = table;
	cursor->storage = storage;
	cursor->index   = index;
	cursor->it      = index_read(index);
	iterator_open(cursor->it, key);

	// jmp if has data
	if (iterator_has(cursor->it))
		return code_at(self->code, op->d);
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
		if (! data_is_array(expr->data))
			error("FROM: array or data type expected");
		cursor->type       = CURSOR_ARRAY;
		cursor->array_pos  = 0;
		cursor->array_data = expr->data;
		data_read_array(&cursor->array_data, &cursor->array_count);
		if (cursor->array_count == 0)
			return ++op;
	} else
	if (expr->type == VALUE_SET)
	{
		cursor->type = CURSOR_SET;
		cursor->set  = (Set*)expr->obj;
		if (cursor->set->list_count == 0)
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
		error("FROM: array or data type expected");
	}

	// jmp on success
	return code_at(self->code, op->c);
}

hot void
ccursor_close(Vm* self, Op* op)
{
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	switch (cursor->type) {
	case CURSOR_TABLE:
		break;
	case CURSOR_ARRAY:
	case CURSOR_SET:
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
		iterator_next(cursor->it);

		// check for eof
		if (! iterator_has(cursor->it))
			return ++op;
		break;
	}
	case CURSOR_ARRAY:
	{
		// array
		cursor->array_pos++;
		if (cursor->array_pos >= cursor->array_count)
			return ++op;
		data_skip(&cursor->array_data);
		break;
	}
	case CURSOR_SET:
	{
		cursor->set_pos++;
		if (cursor->set_pos >= cursor->set->list_count)
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
			error("*: not in active aggregation");
		auto schema  = index_schema(cursor->index);
		auto current = iterator_at(cursor->it);
		assert(current != NULL);
		value_set_data(a, row_data(current, schema), row_data_size(current, schema), NULL);
		break;
	}
	case CURSOR_ARRAY:
	{
		value_read(a, cursor->array_data, NULL);
		break;
	}
	case CURSOR_SET:
	{
		auto ref = &set_at(cursor->set, cursor->set_pos)->value;
		value_copy(a, ref);
		break;
	}
	case CURSOR_GROUP:
	{
		auto node = group_at(cursor->group, cursor->group_pos);
		auto buf  = group_get(cursor->group, node);
		auto msg  = msg_of(buf);
		value_set_data(a, msg->data, msg_data_size(msg), buf);
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
		if (unlikely(! iterator_has(cursor->it)))
			error("*: not in active aggregation");
		auto current = iterator_at(cursor->it);
		assert(current != NULL);
		data     = row_data(current, index_schema(cursor->index));
		data_buf = NULL;
		break;
	}
	case CURSOR_ARRAY:
		data_buf = reg_at(&self->r, cursor->r)->buf;
		data     = cursor->array_data;
		break;
	case CURSOR_SET:
	{
		auto value = &set_at(cursor->set, cursor->set_pos)->value;
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
		if (cursor->type != CURSOR_TABLE)
			error("cursor: unsupported operation");

		auto schema = index_schema(cursor->index);
		int  column_order = op->c;
		if (column_order < schema->column_count)
		{
			auto column = schema_column_of(schema, column_order);
			column_find(column, &data);
		} else
		{
			if (unlikely(! data_is_array(data)))
				error("current cursor object is not an array");

			if (! array_find(&data, column_order))
				error("column <%d> does not exists", column_order);
		}
	}

	// match name in the object
	if (op->d != -1)
	{
		if (unlikely(! data_is_map(data)))
			error("current cursor object type is not a map");

		Str name;
		code_read_string(self->code, op->d, &name);
		if (! map_find_path(&data, &name))
			error("object path '%.*s' not found", str_size(&name),
			      str_of(&name));
	}

	value_read(reg_at(&self->r, op->a), data, data_buf);
}

hot void
ccall(Vm* self, Op* op)
{
	Str name;
	code_read_string(self->code, op->b, &name);

	// prepare arguments
	int    argc = op->c;
	Value* argv[argc];
	for (int i = 0; i < argc; i++)
		argv[i] = stack_at(&self->stack, argc - i);

	// find and call an internal function
	auto func = function_mgr_find(self->function_mgr, &name);
	if (func)
	{
		func->main(self, func, reg_at(&self->r, op->a), argc, argv);
		stack_popn(&self->stack, op->c);
		return;
	}

	// find and call a view
	auto meta = meta_mgr_find(&self->db->meta_mgr, &name, true);
	Callable* callable = meta->iface_data;
	assert(callable != NULL);
	call(self, reg_at(&self->r, op->a), callable, argc, argv);
	stack_popn(&self->stack, op->c);
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table_offset, count]
	auto trx    = self->trx;
	bool unique = op->c;

	// find table/storage by name
	Str name_table;
	code_read_string(self->code, op->a, &name_table);
	auto table   = table_mgr_find(&self->db->table_mgr, &name_table, true);
	auto storage = storage_mgr_find_for(&self->db->storage_mgr, self->shard,
	                                    &table->config->id);

	// write to the storage
	int count = op->b;
	for (int i = 0; i < count; i++)
	{
		auto value = stack_at(&self->stack, count - i);

		if (likely(value->type == VALUE_DATA))
		{
			if (unlikely(!data_is_array(value->data) && !data_is_map(value->data)))
				error("INSERT/REPLACE: array, map or data set expected");

			storage_set(storage, trx, unique, value->data, value->data_size);
		} else
		if (value->type == VALUE_SET)
		{
			auto set = (Set*)value->obj;
			for (int j = 0; j < set->list_count; j++)
			{
				auto ref = &set_at(set, j)->value;
				if (likely(ref->type == VALUE_DATA))
				{
					storage_set(storage, trx, unique, ref->data, ref->data_size);
				} else
				{
					auto buf = buf_create(0);
					value_write(ref, buf);
					storage_set(storage, trx, unique, buf->start, buf_size(buf));
					buf_free(buf);
				}
			}
		} else
		{
			error("INSERT/REPLACE: array, map or data set expected");
		}
	}

	stack_popn(&self->stack, count);
}

hot void
cupdate(Vm* self, Op* op)
{
	// [cursor, count]
	auto trx     = self->trx;
	auto cursor  = cursor_mgr_of(&self->cursor_mgr, op->a);
	auto storage = cursor->storage;

	// update by cursor
	int count = op->b;
	for (int i = 0; i < count; i++)
	{
		auto value = stack_at(&self->stack, count - i);

		if (likely(value->type == VALUE_DATA))
		{
			if (unlikely(!data_is_array(value->data) && !data_is_map(value->data)))
				error("UPDATE: array, map or data set expected");

			storage_update(storage, trx, cursor->it, value->data, value->data_size);
		} else
		if (value->type == VALUE_SET)
		{
			auto set = (Set*)value->obj;
			for (int j = 0; j < set->list_count; j++)
			{
				auto ref = &set_at(set, j)->value;
				if (likely(ref->type == VALUE_DATA))
				{
					storage_update(storage, trx, cursor->it, ref->data, ref->data_size);
				} else
				{
					auto buf = buf_create(0);
					value_write(ref, buf);
					storage_update(storage, trx, cursor->it, buf->start, buf_size(buf));
					buf_free(buf);
				}
			}
		} else
		{
			error("UPDATE: array, map or data set expected");
		}
	}

	stack_popn(&self->stack, count);
}

hot void
cdelete(Vm* self, Op* op)
{
	// delete by cursor
	auto cursor = cursor_mgr_of(&self->cursor_mgr, op->a);
	storage_delete(cursor->storage, self->trx, cursor->it);
}
