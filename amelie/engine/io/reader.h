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
	Chunk* chunk;
	Codec* compression;
	Codec* encryption;
	Buf    buf;
	Buf    buf_decrypted;
	Buf    buf_decompressed;
};

static inline void
reader_init(Reader* self)
{
	self->chunk       = NULL;
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
reader_open(Reader* self, Chunk* chunk)
{
	self->chunk = chunk;

	// get compression context
	auto id = chunk->span.compression;
	if (id != COMPRESSION_NONE)
	{
		self->compression = compression_create(&runtime()->cache_compression, id, 0);
		if (! self->compression)
			error("invalid compression id '%d'", id);
	}

	// get encryption context
	id = chunk->span.encryption;
	if (id != CIPHER_NONE)
	{
		self->encryption = cipher_create(&runtime()->cache_cipher, id,
		                                 &runtime()->random,
		                                 &chunk->source->encryption_key);
		if (! self->encryption)
			error("invalid encryption id '%d'", id);
	}
}

hot static inline Region*
reader_execute(Reader* self, SpanRegion* span_region)
{
	auto chunk = self->chunk;

	buf_reset(&self->buf);
	buf_reset(&self->buf_decrypted);
	buf_reset(&self->buf_decompressed);

	if (chunk_has(chunk, ID))
	{
		// read region data from file
		file_pread_buf(&chunk->file, &self->buf, span_region->size,
		               span_region->offset);
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
		buf_reserve(&self->buf_decompressed, span_region->size_origin);
		codec_decode(self->encryption, &self->buf_decompressed,
		             origin->start, buf_size(origin));
		origin = &self->buf_decompressed;
	}

	// consistency check
	auto region = (Region*)origin->start;
	if (region->size != span_region->size_origin ||
	    region->rows != span_region->rows)
		error("read: region meta data mismatch");

	return region;
}
