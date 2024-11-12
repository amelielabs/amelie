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
	AGG_UNDEF,
	AGG_COUNT,
	AGG_MIN,
	AGG_MAX,
	AGG_SUM,
	AGG_AVG
};

static inline const char*
agg_nameof(int id)
{
	switch (id) {
	case AGG_COUNT:
		return "count";
	case AGG_MIN:
		return "min";
	case AGG_MAX:
		return "max";
	case AGG_SUM:
		return "sum";
	case AGG_AVG:
		return "avg";
	default:
		break;
	}
	return NULL;
}
