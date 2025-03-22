
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

hot void
agg_merge(Value* src, Value* row, int columns, int* aggs)
{
	// merge aggregate states
	for (int col = 0; col < columns; col++)
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
		case AGG_INT_COUNT_DISTINCT:
			// do nothing here
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

		value_set_null(&row[col]);
	}
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
		case AGG_INT_COUNT_DISTINCT:
		{
			// write child set row as [src->keys, col, row_expr]
			assert(self->child);

			// keys
			auto crow = set_reserve(self->child);
			auto keys = self->count_keys;
			for (int key = 0; key < keys; key++)
				value_copy(&crow[key], &src[self->count_columns + key]);

			// column (agg order)
			value_set_int(&crow[keys], col);

			// expr
			value_copy(&crow[keys + 1], &row[col]);
			break;
		}
		case AGG_INT_MIN:
		{
			int64_t value = row[col].integer;
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
			int64_t value = row[col].integer;
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
			int64_t value = row[col].integer;
			if (likely(src[col].type == TYPE_INT))
				src[col].integer += value;
			else
				value_set_int(&src[col], value);
			break;
		}
		case AGG_INT_AVG:
		{
			int64_t value = row[col].integer;
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
			double value = row[col].dbl;
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
			double value = row[col].dbl;
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
			double value = row[col].dbl;
			if (likely(src[col].type == TYPE_DOUBLE))
				src[col].dbl += value;
			else
				value_set_double(&src[col], value);
			break;
		}
		case AGG_DOUBLE_AVG:
		{
			double value = row[col].dbl;
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
