
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_store.h>

hot static inline void
agg_merge_row(Set* self, Value* row, int* aggs)
{
	// get existing or create new row by key
	auto src_ref = set_get(self, &row[self->count_columns], true);
	auto src = set_row(self, src_ref);

	// merge aggregate states
	for (int col = 0; col < self->count_columns; col++)
	{
		if (src[col].type == TYPE_NULL)
		{
			value_move(&src[col], &row[col]);
			continue;
		}
		if (row[col].type == TYPE_NULL)
			continue;

		switch (aggs[col]) {
		case AGG_INT_COUNT:
			src[col].integer += row[col].integer;
			break;
		case AGG_INT_MIN:
			if (row[col].integer < src[col].integer)
				src[col].integer = row[col].integer;
			break;
		case AGG_INT_MAX:
			if (row[col].integer > src[col].integer)
				src[col].integer = row[col].integer;
			break;
		case AGG_INT_SUM:
			src[col].integer += row[col].integer;
			break;
		case AGG_INT_AVG:
			avg_add_int(&src[col].avg, row[col].avg.sum_int,
			             row[col].avg.count);
			break;
		case AGG_DOUBLE_MIN:
			if (row[col].dbl < src[col].dbl)
				src[col].dbl = row[col].dbl;
			break;
		case AGG_DOUBLE_MAX:
			if (row[col].dbl > src[col].dbl)
				src[col].dbl = row[col].dbl;
			break;
		case AGG_DOUBLE_SUM:
			src[col].dbl += row[col].dbl;
			break;
		case AGG_DOUBLE_AVG:
			avg_add_double(&src[col].avg, row[col].avg.sum_double,
			                row[col].avg.count);
			break;
		case AGG_LAMBDA:
			error("distributed operation with lambda is not supported");
			break;
		}
	}

	for (int key = 0; key < self->count_keys; key++)
		value_free(&row[self->count_columns + key]);
}

hot void
agg_merge(Value** list, int list_count, int* aggs)
{
	if (list_count <= 1)
		return;

	auto self = (Set*)list[0]->store;
	for (int i = 1; i < list_count; i++)
	{
		auto set = (Set*)list[i]->store;
		if (set->count == 0)
			continue;

		for (int pos = 0; pos < set->hash.hash_size; pos++)
		{
			auto ref = &set->hash.hash[pos];
			if (ref->row == UINT32_MAX)
				continue;
			auto row = set_row(set, ref->row);
			agg_merge_row(self, row, aggs);
		}
		set->count = 0;
		set->count_rows = 0;
		store_free(&set->store);
		value_set_null(list[i]);
	}
}

static inline int64_t
agg_int_of(Value* row, int col, const char* func)
{
	int64_t value;
	if (likely(row[col].type == TYPE_INT ||
			   row[col].type == TYPE_TIMESTAMP ||
			   row[col].type == TYPE_BOOL))
		value = row[col].integer;
	else
	if (row[col].type == TYPE_DOUBLE)
		value = row[col].dbl;
	else
		error("%s(): unexpected value type", func);
	return value;
}

static inline double
agg_double_of(Value* row, int col, const char* func)
{
	double value;
	if (likely(row[col].type == TYPE_DOUBLE))
		value = row[col].dbl;
	else
	if (row[col].type == TYPE_INT ||
	    row[col].type == TYPE_TIMESTAMP ||
	    row[col].type == TYPE_BOOL)
		value = row[col].integer;
	else
		error("%s(): unexpected value type", func);
	return value;
}

hot void
agg_write(Set* self, Value* row, int src_ref, int* aggs)
{
	// process aggregates values
	auto src = set_row(self, src_ref);
	for (int col = 0; col < self->count_columns; col++)
	{
		if (row[col].type == TYPE_NULL)
			continue;

		switch (aggs[col]) {
		case AGG_INT_COUNT:
		{
			if (likely(src[col].type == TYPE_INT))
				src[col].integer++;
			else
				value_set_int(&src[col], 1);
			break;
		}
		case AGG_INT_MIN:
		{
			int64_t value = agg_int_of(row, col, "min");
			if (likely(src[col].type == TYPE_INT))
			{
				if (value < src[col].integer)
					src[col].integer = value;
			} else {
				value_set_int(&src[col], value);
			}
			break;
		}
		case AGG_INT_MAX:
		{
			int64_t value = agg_int_of(row, col, "max");
			if (likely(src[col].type == TYPE_INT))
			{
				if (value > src[col].integer)
					src[col].integer = value;
			} else {
				value_set_int(&src[col], value);
			}
			break;
		}
		case AGG_INT_SUM:
		{
			int64_t value = agg_int_of(row, col, "sum");
			if (likely(src[col].type == TYPE_INT))
				src[col].integer += value;
			else
				value_set_int(&src[col], value);
			break;
		}
		case AGG_INT_AVG:
		{
			int64_t value = agg_int_of(row, col, "avg");
			if (unlikely(src[col].type == TYPE_NULL))
			{
				src[col].type = TYPE_AVG;
				avg_init(&src[col].avg);
			}
			avg_add_int(&src[col].avg, value, 1);
			break;
		}
		case AGG_DOUBLE_MIN:
		{
			double value = agg_double_of(row, col, "min");
			if (likely(src[col].type == TYPE_DOUBLE))
			{
				if (value < src[col].dbl)
					src[col].dbl = value;
			} else {
				value_set_double(&src[col], value);
			}
			break;
		}
		case AGG_DOUBLE_MAX:
		{
			double value = agg_double_of(row, col, "max");
			if (likely(src[col].type == TYPE_DOUBLE))
			{
				if (value > src[col].dbl)
					src[col].dbl = value;
			} else {
				value_set_double(&src[col], value);
			}
			break;
		}
		case AGG_DOUBLE_SUM:
		{
			double value = agg_double_of(row, col, "sum");
			if (likely(src[col].type == TYPE_DOUBLE))
				src[col].dbl += value;
			else
				value_set_double(&src[col], value);
			break;
		}
		case AGG_DOUBLE_AVG:
		{
			double value = agg_int_of(row, col, "avg");
			if (unlikely(src[col].type == TYPE_NULL))
			{
				src[col].type = TYPE_AVG;
				avg_init(&src[col].avg);
			}
			avg_add_double(&src[col].avg, value, 1);
			break;
		}
		case AGG_LAMBDA:
		{
			value_move(&src[col], &row[col]);
			break;
		}
		}
	}
}
