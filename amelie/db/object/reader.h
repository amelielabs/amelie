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

typedef struct Reader Reader;

struct Reader
{
	Object* object;
	Buf     buf_read;
	Buf     buf;
	Encoder encoder;
};

static inline void
reader_init(Reader* self)
{
	self->object = NULL;
	buf_init(&self->buf_read);
	buf_init(&self->buf);
	encoder_init(&self->encoder);
}

static inline void
reader_reset(Reader* self)
{
	buf_reset(&self->buf_read);
	buf_reset(&self->buf);
	encoder_reset(&self->encoder);
}

static inline void
reader_free(Reader* self)
{
	buf_free(&self->buf_read);
	buf_free(&self->buf);
	encoder_free(&self->encoder);
}

static inline void
reader_open(Reader* self, Object* object)
{
	self->object = object;
	encoder_open(&self->encoder, object->encoding);
}

hot static inline Region*
reader_execute(Reader* self, MetaRegion* meta_region)
{
	auto object  = self->object;
	auto encoder = &self->encoder;
	encoder_reset(encoder);

	if (object_has(object, ID))
	{
		// read region data from file
		file_pread_buf(&object->file, &self->buf_read, meta_region->size,
		               meta_region->offset);
	} else {
		abort();
	}

	// decrypt/decompress or read raw
	Region* region = NULL;
	if (encoder_active(&self->encoder))
	{
		auto buf = &self->buf;
		buf_reset(buf);
		buf_emplace(buf, meta_region->size_origin);
		encoder_decode(encoder, buf->start, buf_size(buf),
		               self->buf_read.start,
		               buf_size(&self->buf_read));
		region = (Region*)self->buf.start;
	} else {
		region = (Region*)self->buf_read.start;
	}

	// consistency check
	if (region->size != meta_region->size_origin ||
	    region->rows != meta_region->rows)
		error("object: region meta data mismatch");
	return region;
}
