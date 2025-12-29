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
	Buf     buf;
	Buf     buf_decrypted;
	Buf     buf_decompressed;
	Codec*  compression;
	Codec*  encryption;
};

static inline void
reader_init(Reader* self)
{
	self->object      = NULL;
	self->compression = NULL;
	self->encryption  = NULL;
	buf_init(&self->buf);
	buf_init(&self->buf_decrypted);
	buf_init(&self->buf_decompressed);
}

static inline void
reader_reset(Reader* self)
{
	if (self->compression)
	{
		codec_cache_push(&runtime()->cache_compression, self->compression);
		self->compression = NULL;
	}
	if (self->encryption)
	{
		codec_cache_push(&runtime()->cache_cipher, self->encryption);
		self->encryption = NULL;
	}
	buf_reset(&self->buf);
	buf_reset(&self->buf_decrypted);
	buf_reset(&self->buf_decompressed);
}

static inline void
reader_free(Reader* self)
{
	buf_free(&self->buf);
	buf_free(&self->buf_decrypted);
	buf_free(&self->buf_decompressed);
}

static inline void
reader_open(Reader* self, Object* object)
{
	self->object = object;

	// get compression context
	auto id = object->meta.compression;
	if (id != COMPRESSION_NONE)
	{
		self->compression = compression_create(&runtime()->cache_compression, id, 0);
		if (! self->compression)
			error("object: invalid compression id '%d'", id);
	}

	// get encryption context
	id = object->meta.encryption;
	if (id != CIPHER_NONE)
	{
		self->encryption = cipher_create(&runtime()->cache_cipher, id,
		                                 &runtime()->random,
		                                 &object->source->encryption_key);
		if (! self->encryption)
			error("object: invalid encryption id '%d'", id);
	}
}

hot static inline Region*
reader_execute(Reader* self, MetaRegion* meta_region)
{
	auto object = self->object;
	buf_reset(&self->buf);
	buf_reset(&self->buf_decrypted);
	buf_reset(&self->buf_decompressed);

	if (object_has(object, ID))
	{
		// read region data from file
		file_pread_buf(&object->file, &self->buf, meta_region->size,
		               meta_region->offset);
	} else {
		abort();
	}
	Buf* origin = &self->buf;

	// decrypt region
	if (self->encryption)
	{
		codec_decode(self->encryption, &self->buf_decrypted,
		             origin->start, buf_size(origin));
		origin = &self->buf_decrypted;
	}

	// decompress region
	if (self->compression)
	{
		buf_reserve(&self->buf_decompressed, meta_region->size_origin);
		codec_decode(self->encryption, &self->buf_decompressed,
		             origin->start, buf_size(origin));
		origin = &self->buf_decompressed;
	}

	// consistency check
	auto region = (Region*)origin->start;
	if (region->size != meta_region->size_origin ||
	    region->rows != meta_region->rows)
		error("object: region meta data mismatch");

	return region;
}
