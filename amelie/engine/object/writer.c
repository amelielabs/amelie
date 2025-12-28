
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

#include <amelie_runtime>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_object.h>

void
writer_init(Writer* self)
{
	self->file        = NULL;
	self->compression = NULL;
	self->encryption  = NULL;
	self->source      = NULL;
	iov_init(&self->iov);
	region_writer_init(&self->region_writer);
	span_writer_init(&self->span_writer);
}

void
writer_free(Writer* self)
{
	writer_reset(self);
	iov_free(&self->iov);
	region_writer_free(&self->region_writer);
	span_writer_free(&self->span_writer);
}

void
writer_reset(Writer* self)
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
	self->source = NULL;
	iov_reset(&self->iov);
	region_writer_reset(&self->region_writer);
	span_writer_reset(&self->span_writer);
}

hot static inline bool
writer_is_region_limit(Writer* self)
{
	if (unlikely(! region_writer_started(&self->region_writer)))
		return true;
	return region_writer_size(&self->region_writer) >= (uint32_t)self->source->region_size;
}

static inline void
writer_start_region(Writer* self)
{
	region_writer_reset(&self->region_writer);
	region_writer_start(&self->region_writer, self->compression,
	                    self->encryption);
}

hot static inline void
writer_stop_region(Writer* self)
{
	if (! region_writer_started(&self->region_writer))
		return;

	// complete region
	region_writer_stop(&self->region_writer);

	// add region to the span
	span_writer_add(&self->span_writer, &self->region_writer,
	                 self->file->size);

	// write region
	iov_reset(&self->iov);
	region_writer_add_to_iov(&self->region_writer, &self->iov);
	file_writev(self->file, iov_pointer(&self->iov), self->iov.iov_count);
}

void
writer_start(Writer* self, Source* source, File* file)
{
	self->source = source;
	self->file   = file;

	// get compression context
	auto id = compression_idof(&source->compression);
	if (id == -1)
		error("invalid compression '%.*s'", str_size(&source->compression),
		      str_of(&source->compression));
	if (id != COMPRESSION_NONE)
		self->compression =
			compression_create(&runtime()->cache_compression, id,
			                   source->compression_level);

	// get encryption context
	id = cipher_idof(&source->encryption);
	if (id == -1)
		error("invalid encryption '%.*s'", str_size(&source->encryption),
		      str_of(&source->encryption));
	if (id != CIPHER_NONE)
		self->encryption =
			cipher_create(&runtime()->cache_cipher, id,
			              &runtime()->random, &source->encryption_key);

	// start new span
	span_writer_reset(&self->span_writer);
	span_writer_start(&self->span_writer, self->compression,
	                  self->encryption,
	                  source->crc);
}

void
writer_stop(Writer*  self, Id* id, uint32_t refreshes,
            uint64_t time_create,
            uint64_t time_refresh,
            uint64_t lsn,
            bool     sync)
{
	if (! span_writer_started(&self->span_writer))
		return;

	if (region_writer_started(&self->region_writer))
		writer_stop_region(self);

	span_writer_stop(&self->span_writer, id, refreshes,
	                 time_create,
	                 time_refresh,
	                 lsn);

	// write span
	iov_reset(&self->iov);
	span_writer_add_to_iov(&self->span_writer, &self->iov);
	file_writev(self->file, iov_pointer(&self->iov), self->iov.iov_count);

	// cleanup
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

	// sync
	if (sync)
		file_sync(self->file);
}

void
writer_add(Writer* self, Row* row)
{
	if (unlikely(writer_is_region_limit(self)))
	{
		// write region
		writer_stop_region(self);

		// start new region
		writer_start_region(self);
	}

	// add row to the region
	region_writer_add(&self->region_writer, row);
}
