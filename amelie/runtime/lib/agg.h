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

typedef union  AggValue AggValue;
typedef struct Agg      Agg;

enum
{
	AGG_UNDEF,
	AGG_COUNT,
	AGG_MIN,
	AGG_MAX,
	AGG_SUM,
	AGG_AVG,
	AGG_FIRST,
	AGG_LAST
};

enum
{
	AGG_NULL,
	AGG_INT,
	AGG_REAL
};

union AggValue
{
	int64_t integer;
	double  real;
};

struct Agg
{
	int      type;
	AggValue value;
	int64_t  div;
};

static inline void
agg_init(Agg* self)
{
	self->type = AGG_NULL;
	self->div  = 0;
	memset(&self->value, 0, sizeof(self->value));
}

static inline int
agg_read(Agg* self, AggValue* value)
{
	if (self->type == AGG_NULL)
		return AGG_NULL;
	*value = self->value;
	if (self->div) 
	{
		if (self->type == AGG_REAL)
			value->real /= self->div;
		else
			value->integer /= self->div;
	}
	return self->type;
}

static inline int
agg_compare(Agg* a, Agg* b)
{
	AggValue a_value;
	auto a_type = agg_read(a, &a_value);

	AggValue b_value;
	auto b_type = agg_read(b, &b_value);

	switch (a_type) {
	case AGG_NULL:
		if (b_type == AGG_NULL)
			return 0;
		return -1;
	case AGG_INT:
		if (b_type == AGG_NULL)
			return 1;
		if (b_type == AGG_INT)
			return compare_int64(a_value.integer, b_value.integer);
		return compare_real((double)a_value.integer, b_value.real);
	case AGG_REAL:
		if (b_type == AGG_NULL)
			return 1;
		if (b_type == AGG_INT)
			return compare_int64((int64_t)a_value.real, b_value.integer);
		return compare_real(a_value.real, b_value.real);
	}

	// unreach
	return 0;
}

hot static inline void
agg_step_min(Agg* self, int type, AggValue* value)
{
	switch (self->type) {
	case AGG_NULL:
		self->type  = type;
		self->value = *value;
		break;
	case AGG_INT:
		if (type == AGG_INT)
		{
			if (value->integer < self->value.integer)
				self->value.integer = value->integer;
		} else
		{
			if (value->real < (double)self->value.integer)
			{
				self->type = AGG_REAL;
				self->value.real = value->real;
			}
		}
		break;
	case AGG_REAL:
		if (type == AGG_INT)
		{
			if ((double)value->integer < self->value.real)
			{
				self->type = AGG_INT;
				self->value.integer = value->integer;
			}
		} else
		{
			if (value->real < self->value.real)
				self->value.real = value->real;
		}
		break;
	}
}

hot static inline void
agg_step_max(Agg* self, int type, AggValue* value)
{
	switch (self->type) {
	case AGG_NULL:
		self->type  = type;
		self->value = *value;
		break;
	case AGG_INT:
		if (type == AGG_INT)
		{
			if (value->integer > self->value.integer)
				self->value.integer = value->integer;
		} else
		{
			if (value->real > (double)self->value.integer)
			{
				self->type = AGG_REAL;
				self->value.real = value->real;
			}
		}
		break;
	case AGG_REAL:
		if (type == AGG_INT)
		{
			if ((double)value->integer > self->value.real)
			{
				self->type = AGG_INT;
				self->value.integer = value->integer;
			}
		} else
		{
			if (value->real > self->value.real)
				self->value.real = value->real;
		}
		break;
	}
}

hot static inline void
agg_step_sum(Agg* self, int type, AggValue* value)
{
	switch (self->type) {
	case AGG_NULL:
		self->type  = type;
		self->value = *value;
		break;
	case AGG_INT:
		if (type == AGG_INT) {
			self->value.integer += value->integer;
		} else
		{
			self->type = AGG_REAL;
			self->value.real = (double)self->value.integer + value->real;
		}
		break;
	case AGG_REAL:
		if (type == AGG_INT)
			self->value.real += value->integer;
		else
			self->value.real += value->real;
		break;
	}
}

hot static inline void
agg_step(Agg* self, int type, int value_type, AggValue* value)
{
	if (value_type == AGG_NULL)
		return;

	switch (type) {
	case AGG_COUNT:
		self->type = AGG_INT;
		self->value.integer++;
		break;
	case AGG_MIN:
		agg_step_min(self, value_type, value);
		break;
	case AGG_MAX:
		agg_step_max(self, value_type, value);
		break;
	case AGG_SUM:
	case AGG_AVG:
		agg_step_sum(self, value_type, value);
		break;
	case AGG_FIRST:
		if (self->type == AGG_NULL)
		{
			self->type  = value_type;
			self->value = *value;
		}
		break;
	case AGG_LAST:
		self->type = value_type;
		self->value = *value;
		break;
	default:
		abort();
		break;
	}

	if (type == AGG_AVG)
		self->div++;
	else
		self->div = 0;
}

hot static inline void
agg_merge(Agg* self, int type, Agg* with)
{
	if (with->type == AGG_NULL)
		return;

	switch (type) {
	case AGG_COUNT:
	{
		self->type = AGG_INT;
		self->value.integer += with->value.integer;
		break;
	}
	case AGG_MIN:
		agg_step_min(self, with->type, &with->value);
		break;
	case AGG_MAX:
		agg_step_max(self, with->type, &with->value);
		break;
	case AGG_SUM:
	case AGG_AVG:
		agg_step_sum(self, with->type, &with->value);
		break;
	case AGG_FIRST:
		if (self->type == AGG_NULL)
		{
			self->type  = with->type;
			self->value = with->value;
		}
		break;
	case AGG_LAST:
		self->type  = with->type;
		self->value = with->value;
		break;
	default:
		abort();
		break;
	}

	if (type == AGG_AVG)
		self->div += with->div;
	else
		self->div = 0;
}
