
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

#include <amelie_base.h>
#include <amelie_os.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_runtime.h>
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
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>

hot static inline void
agg_compute(Agg* self, Value* src, Value* data)
{
	switch (self->type) {
	case AGG_INT_COUNT:
	{
		if (likely(src->type == TYPE_INT))
			src->integer++;
		else
			value_set_int(src, 1);
		break;
	}
	case AGG_INT_MIN:
	{
		int64_t value = data->integer;
		if (likely(src->type == TYPE_INT))
		{
			if (value < src->integer)
				src->integer = value;
		} else {
			value_set_int(src, value);
		}
		break;
	}
	case AGG_INT_MAX:
	{
		int64_t value = data->integer;
		if (likely(src->type == TYPE_INT))
		{
			if (value > src->integer)
				src->integer = value;
		} else {
			value_set_int(src, value);
		}
		break;
	}
	case AGG_INT_SUM:
	{
		int64_t value = data->integer;
		if (likely(src->type == TYPE_INT))
			src->integer += value;
		else
			value_set_int(src, value);
		break;
	}
	case AGG_INT_AVG:
	{
		int64_t value = data->integer;
		if (unlikely(src->type == TYPE_NULL))
		{
			src->type = TYPE_AVG;
			avg_init(&src->avg);
		}
		avg_add_int(&src->avg, value, 1);
		break;
	}
	case AGG_DOUBLE_MIN:
	{
		double value = data->dbl;
		if (likely(src->type == TYPE_DOUBLE))
		{
			if (value < src->dbl)
				src->dbl = value;
		} else {
			value_set_double(src, value);
		}
		break;
	}
	case AGG_DOUBLE_MAX:
	{
		double value = data->dbl;
		if (likely(src->type == TYPE_DOUBLE))
		{
			if (value > src->dbl)
				src->dbl = value;
		} else {
			value_set_double(src, value);
		}
		break;
	}
	case AGG_DOUBLE_SUM:
	{
		double value = data->dbl;
		if (likely(src->type == TYPE_DOUBLE))
			src->dbl += value;
		else
			value_set_double(src, value);
		break;
	}
	case AGG_DOUBLE_AVG:
	{
		double value = data->dbl;
		if (unlikely(src->type == TYPE_NULL))
		{
			src->type = TYPE_AVG;
			avg_init(&src->avg);
		}
		avg_add_double(&src->avg, value, 1);
		break;
	}
	case AGG_LAMBDA:
	{
		value_move(src, data);
		break;
	}
	default:
		abort();
	}
}

hot void
agg_write(Agg* aggs, Value* values, Set* set, int set_row_pos)
{
	// process aggregates values from stack
	auto row = set_row(set, set_row_pos);
	for (int col = 0; col < set->count_columns; col++)
	{
		if (values[col].type == TYPE_NULL)
			continue;

		// track and exclude existing distinct aggregates keys
		if (aggs[col].distinct)
		{
			// [group_by, agg_expr, agg_order] = row_pos

			// group_by
			Value* keys[set->count_keys + 2];
			for (int i = 0; i < set->count_keys; i++)
				keys[i] = set_key(set, set_row_pos, i);

			// agg_expr
			keys[set->count_keys] = &values[col];

			// agg_order
			Value order;
			value_init(&order);
			value_set_int(&order, col);
			keys[set->count_keys + 1] = &order;

			// create or get the distinct record
			bool exists;
			auto ref = set_upsert_ptr(set->distinct_aggs, keys, 0, &exists);
			if (exists)
				continue;

			// save set_row_pos
			value_set_int(set_column(set->distinct_aggs, ref, 0), set_row_pos);
		}

		// compute aggregate
		agg_compute(&aggs[col], &row[col], &values[col]);
	}
}

hot static inline void
agg_merge(Agg* self, Value* a, Value* b)
{
	// merge b into a
	if (b->type == TYPE_NULL)
		return;
	if (a->type == TYPE_NULL)
	{
		value_move(a, b);
		return;
	}
	switch (self->type) {
	case AGG_INT_COUNT:
		a->integer += b->integer;
		break;
	case AGG_INT_MIN:
		if (b->integer < a->integer)
			a->integer = b->integer;
		break;
	case AGG_INT_MAX:
		if (b->integer > a->integer)
			a->integer = b->integer;
		break;
	case AGG_INT_SUM:
		a->integer += b->integer;
		break;
	case AGG_INT_AVG:
		avg_add_int(&a->avg, b->avg.sum_int, b->avg.count);
		break;
	case AGG_DOUBLE_MIN:
		if (b->dbl < a->dbl)
			a->dbl = b->dbl;
		break;
	case AGG_DOUBLE_MAX:
		if (b->dbl > a->dbl)
			a->dbl = b->dbl;
		break;
	case AGG_DOUBLE_SUM:
		a->dbl += b->dbl;
		break;
	case AGG_DOUBLE_AVG:
		avg_add_double(&a->avg, b->avg.sum_double, b->avg.count);
		break;
	case AGG_LAMBDA:
		error("distributed operation on lambda is not supported");
		break;
	default:
		abort();
	}
	value_set_null(b);
}

hot static inline void
agg_merge_set(Agg* aggs, Set* set, Set* with)
{
	// merge aggregates states
	//
	// [group_by] = agg0, agg1, ...
	//
	for (auto pos = 0; pos < with->hash.hash_size; pos++)
	{
		auto at = &with->hash.hash[pos];
		if (at->row == UINT32_MAX)
			continue;

		// get or create aggregate in the set
		auto at_row     = set_row(with, at->row);
		auto at_row_key = set_key(with, at->row, 0);
		auto row_ref    = set_upsert(set, at_row_key, at->hash, NULL);
		auto row        = set_row(set, row_ref);

		// merge states
		for (int col = 0; col < set->count_columns; col++)
		{
			if (aggs[col].distinct)
				continue;
			agg_merge(&aggs[col], &row[col], &at_row[col]);
		}

		// free with row
		for (auto i = 0; i < with->count_columns_row; i++)
			value_free(at_row + i);
	}
}

hot static inline void
agg_merge_set_distinct(Agg* aggs, Set* set, Set* with)
{
	auto distinct_set  = set->distinct_aggs;
	auto distinct_with = with->distinct_aggs;

	// merge distinct_set records
	//
	// [group_by, agg_expr, agg_order] = row_pos
	//
	for (auto pos = 0; pos < distinct_with->hash.hash_size; pos++)
	{
		auto at = &distinct_with->hash.hash[pos];
		if (at->row == UINT32_MAX)
			continue;

		// get or create distinct_aggs record in the set
		bool exists;
		auto at_row = set_row(distinct_with, at->row);
		auto at_key = set_key(distinct_with, at->row, 0);
		auto ref    = set_upsert(distinct_set, at_key, at->hash, &exists);
		if (! exists)
		{
			// for a new distinct record, match and save the
			// original row_pos
			//
			// [group_by] = agg0, ...
			//
			auto row_pos = set_upsert(set, at_key, 0, &exists);
			assert(exists);

			// set the row_pos
			value_set_int(set_column(distinct_set, ref, 0), row_pos);

			// compute aggregate
			auto agg_order = &at_key[distinct_with->count_keys - 1];
			auto agg_expr  = &at_key[distinct_with->count_keys - 2];
			auto state     = set_column(set, row_pos, agg_order->integer);
			agg_compute(&aggs[agg_order->integer], state, agg_expr);
		}

		// free with row
		for (auto i = 0; i < with->count_columns_row; i++)
			value_free(at_row + i);
	}
}

hot void
agg_merge_sets(Agg* aggs, Value** values, int count)
{
	if (! count)
		return;

	// merge all sets into the first one
	auto set = (Set*)values[0]->store;
	if (count > 1)
	{
		for (auto order = 1; order < count; order++)
		{
			auto with = (Set*)values[order]->store;
			if (with->count > 0)
			{
				agg_merge_set(aggs, set, with);
				if (set->distinct_aggs)
					agg_merge_set_distinct(aggs, set, with);
			}
			set_free(with, false);
			value_reset(values[order]);
		}
	}

	// free distinct set
	if (set->distinct_aggs)
	{
		store_free(&set->distinct_aggs->store);
		set->distinct_aggs = NULL;
	}

	// free set hash
	set_hash_free(&set->hash);
}
