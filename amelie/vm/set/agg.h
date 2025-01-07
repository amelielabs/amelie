#pragma once

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

enum
{
	AGG_INT_COUNT,
	AGG_INT_COUNT_DISTINCT,
	AGG_INT_MIN,
	AGG_INT_MAX,
	AGG_INT_SUM,
	AGG_INT_AVG,
	AGG_DOUBLE_MIN,
	AGG_DOUBLE_MAX,
	AGG_DOUBLE_SUM,
	AGG_DOUBLE_AVG,
	AGG_LAMBDA
};

static inline const char*
agg_nameof(int id)
{
	switch (id) {
	case AGG_INT_COUNT:
		return "count";
	case AGG_INT_COUNT_DISTINCT:
		return "count_distinct";
	case AGG_INT_MIN:
	case AGG_DOUBLE_MIN:
		return "min";
	case AGG_INT_MAX:
	case AGG_DOUBLE_MAX:
		return "max";
	case AGG_INT_SUM:
	case AGG_DOUBLE_SUM:
		return "sum";
	case AGG_INT_AVG:
	case AGG_DOUBLE_AVG:
		return "avg";
	}
	return NULL;
}

void agg_merge(Value*, Value*, int, int*);
void agg_write(Set*, Value*, int, int*);
