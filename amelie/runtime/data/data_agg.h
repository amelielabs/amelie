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

always_inline hot static inline int
data_size_agg(Agg* self)
{
	// <AM_AGG> <null|int|real> <div>
	int size = data_size_type();
	switch (self->type) {
	case AGG_NULL:
		size += data_size_null();
		break;
	case AGG_INT:
		size += data_size_integer(self->value.integer);
		break;
	case AGG_REAL:
		size += data_size_real(self->value.real);
		break;
	}
	size += data_size_integer(self->div);
	return size;
}

always_inline hot static inline void
data_read_agg(uint8_t** pos, Agg* agg)
{
	// data type
	int type = **pos;
	if (unlikely(type != AM_AGG))
		data_error(*pos, AM_AGG);
	*pos += data_size_type();

	// null | int | real
	if (data_is_null(*pos))
	{
		data_read_null(pos);
		agg->type = AGG_NULL;
	} else
	if (data_is_integer(*pos))
	{
		data_read_integer(pos, &agg->value.integer);
		agg->type = AGG_INT;
	} else
	{
		data_read_real(pos, &agg->value.real);
		agg->type = AGG_REAL;
	}

	// div
	data_read_integer(pos, &agg->div);
}

always_inline hot static inline void
data_write_agg(uint8_t** pos, Agg* agg)
{
	// data type
	uint8_t* data = *pos;
	*data = AM_AGG;
	*pos += data_size_type();

	// null | int | real
	switch (agg->type) {
	case AGG_NULL:
		data_write_null(pos);
		break;
	case AGG_INT:
		data_write_integer(pos, agg->value.integer);
		break;
	case AGG_REAL:
		data_write_real(pos, agg->value.real);
		break;
	}

	// div
	data_write_integer(pos, agg->div);
}

always_inline hot static inline bool
data_is_agg(uint8_t* data)
{
	return *data == AM_AGG;
}
