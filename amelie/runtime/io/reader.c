
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

#include <amelie_runtime.h>
#include <amelie_io.h>

void
reader_init(Reader* self)
{
	self->read           = NULL;
	self->readahead_size = 256 * 1024;
	self->offset         = 0;
	self->offset_file    = 0;
	self->limit          = 0;
	self->limit_size     = 0;
	buf_init(&self->readahead);
	file_init(&self->file);
}

void
reader_reset(Reader* self)
{
	self->read        = NULL;
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

hot static void
reader_read_line(Reader* self, Str* in, Str* out)
{
	unused(self);
	auto pos = in->pos;
	auto end = in->end;
	while (pos < end && *pos != '\n')
		pos++;
	if (pos == end)
		return;
	// including \n
	str_set(out, in->pos, (pos - in->pos) + 1);
}

void
reader_open(Reader* self, ReaderType type, char* path,
            int     limit,
            int     limit_size)
{
	self->type = type;
	switch (type) {
	case READER_LINE:
		// read rows separated by \n
		self->read = reader_read_line;
		break;
	}
	self->limit      = limit;
	self->limit_size = limit_size;
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
	auto readahead = &self->readahead;
	auto buf = buf_create();
	errdefer_buf(buf);

	*count = 0;
	for (;;)
	{
		Str in =
		{
			.pos       = (char*)readahead->start + self->offset,
			.end       = (char*)readahead->position,
			.allocated = false
		};

		Str out;
		str_init(&out);
		self->read(self, &in, &out);

		if (likely(! str_empty(&out)))
		{
			buf_write_str(buf, &out);
			self->offset += str_size(&out);

			(*count)++;
			if ((*count) == self->limit)
				break;
			if (buf_size(buf) >= self->limit_size)
				break;
			continue;
		}

		// need more data
		if (str_empty(&in))
		{
			buf_reset(readahead);
		} else
		{
			// move data and truncate readahead
			int left = str_size(&in);
			memmove(readahead->start, in.pos, left);
			readahead->position = readahead->start + left;
		}
		self->offset = 0;

		// readahead
		buf_reserve(readahead, self->readahead_size);
		auto to_read = file_read_raw(&self->file, readahead->position,
		                             self->readahead_size);
		buf_advance(readahead, to_read);
		self->offset_file += to_read;
		if (likely(to_read > 0))
			continue;

		// eof
		if (buf_empty(readahead))
		{
			// done (all data read)
			if (buf_empty(buf))
				return NULL;

			// done (with data)
			break;
		}

		// data left in the readahead
		if (self->type == READER_LINE)
		{
			buf_write(buf, readahead->start, buf_size(readahead));
			self->offset += buf_size(readahead);
			break;
		}

		error("incomplete JSON value");
	}

	return buf;
}
