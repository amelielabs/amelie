
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

Writer*
writer_allocate(void)
{
	auto self = (Writer*)am_malloc(sizeof(Writer));
	self->file        = NULL;
	self->compression = NULL;
	self->encryption  = NULL;
	self->source      = NULL;
	iov_init(&self->iov);
	region_writer_init(&self->region_writer);
	meta_writer_init(&self->meta_writer);
	hash_writer_init(&self->hash_writer);
	list_init(&self->link);
	return self;
}

void
writer_free(Writer* self)
{
	writer_reset(self);
	iov_free(&self->iov);
	region_writer_free(&self->region_writer);
	meta_writer_free(&self->meta_writer);
	hash_writer_free(&self->hash_writer);
	am_free(self);
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
	meta_writer_reset(&self->meta_writer);
	hash_writer_reset(&self->hash_writer);
	list_init(&self->link);
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

	// add region to the meta data
	meta_writer_add(&self->meta_writer, &self->region_writer, self->file->size);

	// write region
	iov_reset(&self->iov);
	region_writer_add_to_iov(&self->region_writer, &self->iov);
	file_writev(self->file, iov_pointer(&self->iov), self->iov.iov_count);
}

void
writer_start(Writer* self, Source* source, File* file, Keys* keys, bool hash_partition)
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

	// start new meta data
	meta_writer_reset(&self->meta_writer);
	meta_writer_start(&self->meta_writer, self->compression,
	                  self->encryption, source->crc);

	// prepare hash writer
	hash_writer_prepare(&self->hash_writer, keys, hash_partition);
}

void
writer_stop(Writer*  self,
            Id*      id,
            uint64_t time_create,
            uint64_t lsn)
{
	if (! meta_writer_started(&self->meta_writer))
		return;

	// complete last region and meta data
	if (region_writer_started(&self->region_writer))
		writer_stop_region(self);

	// use hash partitions min/max
	uint32_t hash_min = 0;
	uint32_t hash_max = 0;
	if (self->hash_writer.active)
	{
		hash_min = self->hash_writer.hash_min;
		hash_max = self->hash_writer.hash_max;
	}
	meta_writer_stop(&self->meta_writer, id, hash_min, hash_max,
	                 time_create, lsn);

	// write meta data
	iov_reset(&self->iov);
	meta_writer_add_to_iov(&self->meta_writer, &self->iov);
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

	// calculate row hash and track hash partitions
	hash_writer_add(&self->hash_writer, row);
}
