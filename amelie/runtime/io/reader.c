
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

#include <amelie_runtime.h>
#include <amelie_io.h>

void
reader_init(Reader* self)
{
	self->readahead_size = 256 * 1024;
	self->offset      = 0;
	self->offset_file = 0;
	self->limit       = 0;
	self->limit_size  = 0;
	buf_init(&self->readahead);
	file_init(&self->file);
}

void
reader_reset(Reader* self)
{
	self->offset      = 0;
	self->offset_file = 0;
	self->limit       = 0;
	self->limit_size  = 0;
	file_close(&self->file);
	buf_reset(&self->readahead);
}

void
reader_free(Reader* self)
{
	reader_reset(self);
	buf_free(&self->readahead);
}

void
reader_open(Reader* self, const char* path, int limit, int limit_size)
{
	self->limit = limit;
	self->limit_size = limit_size;
	buf_reserve(&self->readahead, self->readahead_size);
	if (path)
		file_open(&self->file, path);
	else
		file_open_stdin(&self->file);
}

void
reader_close(Reader* self)
{
	reader_reset(self);
}

hot Buf*
reader_read(Reader* self, int* count)
{
	auto buf = buf_create();
	guard_buf(buf);

	auto limit = self->limit;
	*count = 0;

	for (;;)
	{
		auto start = self->readahead.start + self->offset;
		auto pos   = start;
		auto end   = self->readahead.position;

		while (pos < end && *pos != '\n')
			pos++;

		int size = pos - start;
		if (size > 0)
		{
			buf_write(buf, start, size);
			self->offset += size;
		}

		if (pos == end)
		{
			self->offset = 0;
			buf_reset(&self->readahead);

			// readahead
			auto to_read = file_read_raw(&self->file, self->readahead.start,
			                              self->readahead_size);
			buf_advance(&self->readahead, to_read);
			self->offset_file += to_read;

			// eof
			if (buf_empty(&self->readahead))
			{
				if (! buf_empty(buf))
					break;
				return NULL;
			}
		} else
		{
			// \n
			self->offset++;
			pos++;

			// skip empty lines
			if (! size)
				continue;
			buf_write(buf, "\n", 1);
			(*count)++;

			limit--;
			if (limit == 0)
				break;
			if (buf_size(buf) >= self->limit_size)
				break;
		}
	}

	unguard();
	return buf;
}
