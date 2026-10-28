
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
#include <amelie_commit.h>
#include <amelie_func.h>
#include <amelie_vm.h>

hot Op*
ctable_open(Vm* self, Op* op)
{
	// [cursor, open_offset, _eof]
	auto open = open_at(&self->program->code_data, op->b);
	auto part = open->table->part;

	// create key for index scan
	auto buf = buf_create();
	defer_buf(buf);

	Row* key = NULL;
	if (open->index)
	{
		auto keys = &open->index->keys;
		auto keys_count = open->keys_count;
		key = row_create_key(buf, keys, stack_at(&self->stack, keys_count),
		                     keys_count);
		stack_popn(&self->stack, keys_count);

		// in case of hash index, use key only for point-lookup
		if (open->index->type == INDEX_HASH && !open->point_lookup)
			key = NULL;
	}

	// create iterator
	auto it = cursor_open(part, open->index, open->point_lookup, key);

	// set cursor
	auto cursor = reg_at(&self->r, op->a);
	value_set_cursor(cursor, open->table, it);

	// jmp to next op if has data
	if (likely(iterator_has(cursor->cursor)))
		return ++op;

	// jmp on eof
	return code_at(self->code, op->c);
}

hot void
cinsert(Vm* self, Op* op)
{
	// [table*, store, refs]
	auto table   = (Table*)op->a;
	auto part    = table->part;
	auto columns = table_columns(table);
	auto store   = reg_at(&self->r, op->b);

	// set refs
	auto refs    = stack_at(&self->stack, op->c);

	// using local context for random/time source
	auto random = &self->gtr->local->random;
	auto time   =  self->gtr->local->time_ms;

	Value identity;
	value_init(&identity);

	auto tr = tr_list_last(&table->user->track.prepared);
	auto it = store_iterator(store->store);
	defer(store_iterator_close, it);

	for (Value* value;
	    (value = store_iterator_at(it));
	     store_iterator_next(it))
	{
		if (columns->identity)
			row_identity(columns->identity, refs, value, &identity, random, time);

		auto row = row_create(part, columns, value, refs, &identity);
		part_insert(part, tr, row);
	}

	if (op->c)
		stack_popn(&self->stack, op->c);

	value_free(store);
}

hot void
cupsert_open(Vm* self, Op* op)
{
	// [cursor, table*]
	auto cursor = reg_at(&self->r, op->a);
	auto table  = (Table*)op->b;
	auto part   = table->part;

	// create primary index iterator for related partition
	auto primary = part_primary(part);
	if (unlikely(! primary))
		error("table {str}.{str}: primary key is required for upsert",
		      table->rel.user, table->rel.name);
	auto it = index_iterator(primary);
	value_set_cursor(cursor, table, it);
}

Op*
cupsert(Vm* self, Op* op)
{
	// [cursor, _jmp, _jmp_returning, store, refs]
	auto cursor = reg_at(&self->r, op->a);
	assert(cursor->type == TYPE_CURSOR);
	auto table  = cursor->table;
	auto columns = table_columns(table);

	// using local context for random/time source
	auto random = &self->gtr->local->random;
	auto time   =  self->gtr->local->time_ms;

	// create values iterator
	auto store = reg_at(&self->r, op->d);
	auto refs = stack_at(&self->stack, op->e);
	if (self->insert) {
		store_iterator_next(self->insert);
	} else
	{
		self->insert = store_iterator(store->store);
		self->insert_refs = refs;
	}

	Value identity;
	value_init(&identity);

	auto tr = tr_list_last(&table->user->track.prepared);
	for (Value* value;
	    (value = store_iterator_at(self->insert));
	     store_iterator_next(self->insert))
	{
		// create row
		if (columns->identity)
			row_identity(columns->identity, refs, value, &identity, random, time);
		auto row = row_create(table->part, columns, value, refs, &identity);

		// insert or get (open iterator in both cases)
		auto exists = part_upsert(table->part, tr, cursor->cursor, row);
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

	// cleanup
	store_iterator_close(self->insert);
	self->insert = NULL;
	self->insert_refs = NULL;
	value_free(cursor);
	value_free(store);
	return ++op;
}

hot void
cdelete(Vm* self, Op* op)
{
	// [cursor]
	auto cursor = reg_at(&self->r, op->a);

	auto table = cursor->table;
	auto tr = tr_list_last(&table->user->track.prepared);

	// delete by cursor
	part_delete(table->part, tr, cursor->cursor);
}

hot void
cupdate(Vm* self, Op* op)
{
	// [cursor, order/value count]
	auto cursor = reg_at(&self->r, op->a);
	assert(cursor->type == TYPE_CURSOR);

	auto table = cursor->table;
	auto tr = tr_list_last(&table->user->track.prepared);

	auto row_src = iterator_at(cursor->cursor);
	auto row_values = stack_at(&self->stack, op->b * 2);
	auto row = row_update(table->part,
	                      table_columns(table), row_src,
	                      row_values, op->b);
	part_update(table->part, tr, cursor->cursor, row);
	stack_popn(&self->stack, op->b * 2);
}

void
cclose(Vm* self, Op* op)
{
	unused(self);
	unused(op);
	/*
	if (self->allow_close)
		dispatch_close(&self->gtr->dispatch);
	*/
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
cmatching(Vm* self, Op* op)
{
	// [set, table, column, to, top]
	auto table   = (Table*)op->b;
	auto column  = (Column*)op->c;
	auto columns = table_columns(table);
	auto part    = table->part;
	auto flat    = flats_at(&part->flats, column);

	auto vector = reg_at(&self->r, op->d);
	if (vector->type != TYPE_VECTOR)
		error("MATCHING: vector expression expected");
	if (vector->vector_dim != (column->size_flat / sizeof(float)))
		error("MATCHING: vector dimension mismatch");

	// vector search
	auto matching = matching_create(columns, part->heap, flat, op->e);
	defer(matching_free, matching);
	matching_execute(matching, vector->vector);
	matching_result(matching, reg_at(&self->r, op->a));
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
	vm_init(&vm, self->quota);
	defer(vm_free, &vm);
	vm_run(&vm, self->gtr, NULL, program, argv, &ret, false);
	if (ret.value)
		value_move(result, ret.value);

	stack_popn(&self->stack, argc);
}
