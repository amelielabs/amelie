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

typedef struct AggInt AggInt;

struct AggInt
{
	uint8_t type:7;
	uint8_t value_null:1;
	int64_t value;
	int64_t div;
} packed;

static inline void
agg_int_init(AggInt* self, int type)
{
	self->type       = type;
	self->value_null = true;
	self->value      = 0;
	self->div        = 0;
}

static inline bool
agg_int_read(AggInt* self, int64_t* value)
{
	if (self->type == AGG_INT_COUNT)
	{
		*value = self->value;
		return true;
	}
	if (self->value_null)
		return false;
	*value = self->value;
	if (self->type == AGG_INT_AVG)
		*value /= self->div;
	return true;
}

static inline int
agg_int_compare(AggInt* a, AggInt* b)
{
	int64_t a_value;
	auto    a_value_null = agg_int_read(a, &a_value);
	int64_t b_value;
	auto    b_value_null = agg_int_read(b, &b_value);
	if (a_value_null || b_value_null)
		return a_value_null - b_value_null;
	return compare_int64(a_value, b_value);
}

hot static inline void
agg_int_step(AggInt* self, int64_t value)
{
	if (unlikely(self->value_null))
	{
		self->value      = value;
		self->value_null = false;
		self->div        = 1;
		return;
	}
	switch (self->type) {
	case AGG_INT_COUNT:
		self->value++;
		break;
	case AGG_INT_MIN:
		if (value < self->value)
			self->value = value;
		break;
	case AGG_INT_MAX:
		if (value > self->value)
			self->value = value;
		break;
	case AGG_INT_SUM:
	case AGG_INT_AVG:
		self->value += value;
		break;
	default:
		abort();
		break;
	}
	self->div++;
}

hot static inline void
agg_int_merge(AggInt* self, AggInt* with)
{
	if (unlikely(self->type != with->type))
		error("aggregate types mismatch");
	if (with->value_null)
		return;
	switch (self->type) {
	case AGG_INT_COUNT:
		self->value += with->value;
		break;
	case AGG_INT_MIN:
		if (with->value < self->value)
			self->value = with->value;
		break;
	case AGG_INT_MAX:
		if (with->value > self->value)
			self->value = with->value;
		break;
	case AGG_INT_SUM:
	case AGG_INT_AVG:
		self->value += with->value;
		break;
	default:
		abort();
		break;
	}
	self->div += with->div;
}
