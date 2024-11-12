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

typedef struct AggDouble AggDouble;

struct AggDouble
{
	uint8_t type:7;
	uint8_t value_null:1;
	double  value;
	int64_t div;
} packed;

static inline void
agg_double_init(AggDouble* self, int type)
{
	self->type       = type;
	self->value_null = true;
	self->value      = 0;
	self->div        = 0;
}

static inline bool
agg_double_read(AggDouble* self, double* value)
{
	if (self->type == AGG_COUNT)
	{
		*value = self->value;
		return true;
	}
	*value = self->value;
	if (self->type == AGG_AVG)
		*value /= self->div;
	return true;
}

static inline int
agg_double_compare(AggDouble* a, AggDouble* b)
{
	double a_value;
	auto   a_value_null = agg_double_read(a, &a_value);
	double b_value;
	auto   b_value_null = agg_double_read(b, &b_value);
	if (a_value_null || b_value_null)
		return a_value_null - b_value_null;
	return compare_real(a_value, b_value);
}

hot static inline void
agg_double_step(AggDouble* self, double value)
{
	if (unlikely(self->value_null))
	{
		self->value      = value;
		self->value_null = false;
		self->div        = 1;
		return;
	}
	switch (self->type) {
	case AGG_COUNT:
		self->value++;
		break;
	case AGG_MIN:
		if (value < self->value)
			self->value = value;
		break;
	case AGG_MAX:
		if (value > self->value)
			self->value = value;
		break;
	case AGG_SUM:
	case AGG_AVG:
		self->value += value;
		break;
	default:
		abort();
		break;
	}
	self->div++;
}

hot static inline void
agg_double_merge(AggDouble* self, AggDouble* with)
{
	if (unlikely(self->type != with->type))
		error("aggregate types mismatch");
	if (with->value_null)
		return;
	switch (self->type) {
	case AGG_COUNT:
		self->value += with->value;
		break;
	case AGG_MIN:
		if (with->value < self->value)
			self->value = with->value;
		break;
	case AGG_MAX:
		if (with->value > self->value)
			self->value = with->value;
		break;
	case AGG_SUM:
	case AGG_AVG:
		self->value += with->value;
		break;
	default:
		abort();
		break;
	}
	self->div += with->div;
}
