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
data_size_vector(int size)
{
	return data_size_type() + sizeof(uint32_t) + sizeof(float) * size;
}

always_inline hot static inline void
data_read_vector(uint8_t** pos, Vector* vector)
{
	if (unlikely(**pos != AM_VECTOR))
		data_error(*pos, AM_VECTOR);
	*pos += data_size_type();
	vector->size = *(uint32_t*)*pos;
	*pos += sizeof(uint32_t);
	vector->value = (float*)*pos;
	*pos += sizeof(float) * vector->size;
}

always_inline hot static inline void
data_write_vector(uint8_t** pos, Vector* vector)
{
	uint8_t* data = *pos;
	*data = AM_VECTOR;
	*pos += data_size_type();
	*(uint32_t*)(*pos) = vector->size;
	*pos += sizeof(uint32_t);
	memcpy(*pos, vector->value, sizeof(float) * vector->size);
	*pos += sizeof(float) * vector->size;
}

always_inline hot static inline void
data_write_vector_size(uint8_t** pos, int size)
{
	uint8_t* data = *pos;
	*data = AM_VECTOR;
	*pos += data_size_type();
	*(uint32_t*)(*pos) = size;
	*pos += sizeof(uint32_t);
}

always_inline hot static inline bool
data_is_vector(uint8_t* data)
{
	return *data == AM_VECTOR;
}
