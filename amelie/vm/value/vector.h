#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

static inline Buf*
vector_create(Vector* self, int size)
{
	auto buf = buf_begin();
	buf_reserve(buf, data_size_vector(size));
	data_write_vector_size(&buf->position, 0);
	buf_end(buf);
	auto pos = buf->start;
	data_read_vector(&pos, self);
	self->size = size;
	return buf;
}
