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

static inline Buf*
vector_create(Vector* self, int size)
{
	auto buf = buf_create();
	buf_reserve(buf, data_size_vector(size));
	data_write_vector_size(&buf->position, 0);
	auto pos = buf->start;
	data_read_vector(&pos, self);
	self->size = size;
	return buf;
}
