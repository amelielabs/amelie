
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>

hot void
csend_shard(Vm* self, Op* op)
{
	// [rdispatch, store, refs, offset]
	auto send  = send_at(self->code_data, op->d);
	auto table = send->table;

	// create dispatch
	auto dtr = self->dtr;
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (send->has_result)
		dispatch_set_returning(dispatch);

	if (self->allow_close &&
	    self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// redistribute rows between partitions
	auto refs  = stack_at(&self->stack, op->c);
	auto store = reg_at(&self->r, op->b)->store;
	if (store->type == STORE_SET)
	{
		auto set = (Set*)store;
		for (auto order = 0; order < set->count_rows; order++)
		{
			auto row      = set_row(set, order);
			auto identity = row_get_identity(table, refs, row);
			auto part     = row_map(table, refs, row, identity);

			auto req = dispatch_find(dispatch, part);
			if (! req)
			{
				req = dispatch_add(dispatch, &dispatch_mgr->cache_req,
				                   REQ_EXECUTE,
				                   send->start,
				                   &self->program->code_backend,
				                   &self->program->code_data,
				                   part);
				if (op->c > 0)
					req_copy_refs(req, refs, op->c);
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
			auto part     = row_map(table, refs, row, identity);

			auto req = dispatch_find(dispatch, part);
			if (! req)
			{
				req = dispatch_add(dispatch, &dispatch_mgr->cache_req,
				                   REQ_EXECUTE,
				                   send->start,
				                   &self->program->code_backend,
				                   &self->program->code_data,
				                   part);
				if (op->c > 0)
					req_copy_refs(req, refs, op->c);
			}
			buf_write(&req->arg, &row, sizeof(Value*));
			buf_write(&req->arg, &identity, sizeof(int64_t));
		}
	}
	if (op->c > 0)
		stack_popn(&self->stack, op->c);
	value_free(reg_at(&self->r, op->b));

	// execute
	executor_send(share()->executor, dtr, dispatch);

	// return dispatch order
	value_set_int(reg_at(&self->r, op->a), dispatch->order);
}

hot void
csend_lookup(Vm* self, Op* op)
{
	// [rdispatch, refs, offset]
	auto send  = send_at(self->code_data, op->c);
	auto table = send->table;
	auto index = table_primary(table);

	// create dispatch
	auto dtr = self->dtr;
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (send->has_result)
		dispatch_set_returning(dispatch);

	if (self->allow_close &&
	    self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// map partition using keys
	auto part = row_map_keys(table, stack_at(&self->stack, index->keys.list_count));
	stack_popn(&self->stack, index->keys.list_count);

	auto req  = dispatch_add(dispatch, &dispatch_mgr->cache_req,
	                         REQ_EXECUTE,
	                         send->start,
	                         &self->program->code_backend,
	                         &self->program->code_data,
	                         part);
	if (op->b > 0)
	{
		req_copy_refs(req, stack_at(&self->stack, op->b), op->b);
		stack_popn(&self->stack, op->b);
	}

	// execute
	executor_send(share()->executor, dtr, dispatch);

	// return dispatch order
	value_set_int(reg_at(&self->r, op->a), dispatch->order);
}

hot void
csend_all(Vm* self, Op* op)
{
	// [rdispatch, refs, offset]
	auto send  = send_at(self->code_data, op->c);
	auto table = send->table;

	// create dispatch
	auto dtr = self->dtr;
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	if (send->has_result)
		dispatch_set_returning(dispatch);

	if (self->allow_close &&
	    self->program->send_last == code_posof(self->code, op))
		dispatch_set_close(dispatch);

	// send to all table backends
	list_foreach(&table->engine.levels->list)
	{
		auto part = list_at(Part, link);
		auto req = dispatch_add(dispatch, &dispatch_mgr->cache_req,
		                        REQ_EXECUTE,
		                        send->start,
		                        &self->program->code_backend,
		                        &self->program->code_data,
		                        part);
		if (op->b > 0)
			req_copy_refs(req, stack_at(&self->stack, op->b), op->b);
	}
	if (op->b > 0)
		stack_popn(&self->stack, op->b);

	// execute
	executor_send(share()->executor, dtr, dispatch);

	// return dispatch order
	value_set_int(reg_at(&self->r, op->a), dispatch->order);
}

void
cclose(Vm* self, Op* op)
{
	unused(op);
	if (self->allow_close)
		dispatch_mgr_close(&self->dtr->dispatch_mgr);
}

void
cunion_limit(Vm* self, Op* op)
{
	// [union, limit]

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

	auto uni = (Union*)reg_at(&self->r, op->a)->store;
	union_set_limit(uni, limit);
}

void
cunion_offset(Vm* self, Op* op)
{
	// [union, offset]

	// offset
	int64_t offset = 0;
	if (op->b != -1)
	{
		if (unlikely(reg_at(&self->r, op->b)->type != TYPE_INT))
			error("OFFSET: integer type expected");
		offset = reg_at(&self->r, op->b)->integer;
		if (unlikely(offset < 0))
			error("OFFSET: positive integer value expected");
	}

	auto uni = (Union*)reg_at(&self->r, op->a)->store;
	union_set_offset(uni, offset);
}

hot void
crecv_aggs(Vm* self, Op* op)
{
	// [set, rdispatch, aggs]
	auto aggs = (Agg*)code_data_at(self->code_data, op->c);

	// get dispatch
	auto dispatch_order = reg_at(&self->r, op->b)->integer;
	auto dispatch = dispatch_mgr_at(&self->dtr->dispatch_mgr, dispatch_order);
	assert(dispatch);

	// wait for group completion
	dispatch_wait(dispatch);

	Value* values[dispatch->list_count];
	int    values_count = 0;

	// collect results
	auto error = (Req*)NULL;
	list_foreach(&dispatch->list)
	{
		auto req = list_at(Req, link);
		if (req->error && !error)
			error = req;
		auto value = &req->result;
		if (value->type == TYPE_STORE)
		{
			values[values_count] = value;
			values_count++;
		}
	}
	if (error)
		rethrow_buf(error->error);

	// merge aggregate sets into the one set (first)
	assert(values_count > 0);
	agg_merge_sets(aggs, values, values_count);

	// return first set
	auto first = values[0];
	value_set_store(reg_at(&self->r, op->a), first->store);
	value_reset(first);
}

hot void
crecv(Vm* self, Op* op)
{
	// [runion, rdispatch]
	auto result = (Union*)reg_at(&self->r, op->a)->store;

	// get dispatch
	auto dispatch_order = reg_at(&self->r, op->b)->integer;
	auto dispatch = dispatch_mgr_at(&self->dtr->dispatch_mgr, dispatch_order);
	assert(dispatch);

	// wait for group completion
	dispatch_wait(dispatch);

	// collect result and to the union
	auto error = (Req*)NULL;
	list_foreach(&dispatch->list)
	{
		auto req = list_at(Req, link);
		if (req->error && !error)
			error = req;
		auto value = &req->result;
		if (value->type == TYPE_STORE)
		{
			union_add(result, value->store);
			value_reset(value);
		}
	}
	if (error)
		rethrow_buf(error->error);
}

void
cvar_set(Vm* self, Op* op)
{
	// [var, is_arg, value, column]
	Value* var;
	if (op->b)
		var = &self->args[op->a];
	else
		var = stack_get(&self->stack, op->a);
	value_free(var);

	// value
	auto src = reg_at(&self->r, op->c);
	if (src->type != TYPE_STORE)
	{
		value_copy(var, src);
		return;
	}

	// set or union
	auto store = src->store;
	if (store->type == STORE_SET)
	{
		auto set = (Set*)store;
		if (set->count >= 1)
			value_copy(var, set_row_of(set, 0) + op->d);
	} else
	{
		auto it = store_iterator(store);
		defer(store_iterator_close, it);
		if (store_iterator_has(it))
			value_copy(var, it->current + op->d);
	}
}

void
cfirst(Vm* self, Op* op)
{
	// [result, store]
	auto result = reg_at(&self->r, op->a);
	value_set_null(result);

	// set or union
	auto src   = reg_at(&self->r, op->b);
	auto store = src->store;
	if (store->type == STORE_SET)
	{
		auto set = (Set*)store;
		if (set->count >= 1)
			value_move(result, set_row_of(set, 0));
	} else
	{
		auto it = store_iterator(store);
		defer(store_iterator_close, it);
		if (store_iterator_has(it))
			value_move(result, it->current);
	}
	value_free(src);
}

hot Op*
ctable_open(Vm* self, Op* op, bool point_lookup, bool open_part)
{
	// [cursor, name_offset, _eof, keys_count]

	// read names
	uint8_t* pos = code_data_at(self->code_data, op->b);
	Str name_db;
	Str name_table;
	Str name_index;
	json_read_string(&pos, &name_db);
	json_read_string(&pos, &name_table);
	json_read_string(&pos, &name_index);

	// find table, partition and index
	auto table = table_mgr_find(&share()->db->catalog.table_mgr, &name_db, &name_table, true);
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
		cursor->part = self->part;
	else
		cursor->part = NULL;
	cursor->cursor = engine_iterator(&table->engine, cursor->part, index, point_lookup, key_ref);
	cursor->table  = table;
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
	cursor->part   = self->part;
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
	auto part    = self->part;
	auto columns = table_columns(table);

	// insert
	auto pos = self->code_arg->start;
	auto end = self->code_arg->position;
	while (pos < end)
	{
		auto value    = *(Value**)pos;
		auto identity = *(int64_t*)(pos + sizeof(Value*));
		pos += sizeof(Value*) + sizeof(int64_t);

		auto row = row_create(part->heap, columns, value, self->refs, identity);
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

		auto row = row_create(cursor->part->heap, columns, value, self->refs, identity);

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
	auto row = row_update(cursor->part->heap, row_src, table_columns(cursor->table),
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

hot void
ccall_udf(Vm* self, Op* op)
{
	// [result, udf*]
	auto udf = (Udf*)op->b;
	auto program = (Program*)udf->data;

	auto argc   = udf->config->args.count;
	auto argv   = stack_at(&self->stack, argc);
	auto result = reg_at(&self->r, op->a);
	value_set_null(result);

	Return ret;
	return_init(&ret);

	// execute udf
	Vm vm;
	vm_init(&vm, self->part, self->dtr);
	defer(vm_free, &vm);
	reg_prepare(&vm.r, program->code.regs);

	vm_run(&vm, self->local, self->tr,
	       program, &program->code, &program->code_data, NULL,
	       NULL, // refs
	       argv,
	       &ret,
	       false,
		   0);

	if (ret.value)
		value_move(result, ret.value);

	stack_popn(&self->stack, argc);
}
