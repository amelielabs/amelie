
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
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>

Writer*
writer_allocate(void)
{
	auto self = (Writer*)am_malloc(sizeof(Writer));
	self->file        = NULL;
	self->region_size = 0;
	self->storage     = NULL;
	self->next        = NULL;
	region_writer_init(&self->region_writer);
	meta_writer_init(&self->meta_writer);
	return self;
}

void
writer_free(Writer* self)
{
	writer_reset(self);
	region_writer_free(&self->region_writer);
	meta_writer_free(&self->meta_writer);
	am_free(self);
}

void
writer_reset(Writer* self)
{
	self->file        = NULL;
	self->region_size = 0;
	self->storage     = NULL;
	self->next        = NULL;
	region_writer_reset(&self->region_writer);
	meta_writer_reset(&self->meta_writer);
}

hot static inline bool
writer_is_region_limit(Writer* self)
{
	if (unlikely(! region_writer_started(&self->region_writer)))
		return true;
	return (int)region_writer_size(&self->region_writer) >= self->region_size;
}

static inline void
writer_start_region(Writer* self)
{
	region_writer_reset(&self->region_writer);
	region_writer_start(&self->region_writer);
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
	auto encoder = &self->region_writer.encoder;
	auto iov = encoder_iov(encoder);
	auto iov_count = encoder_iov_count(encoder);
	file_writev(self->file, iov, iov_count);
}

void
writer_start(Writer* self, File* file, Storage* storage, int region_size)
{
	self->file        = file;
	self->storage     = storage;
	self->region_size = region_size;

	// set compression
	meta_writer_open(&self->meta_writer, storage);
	region_writer_open(&self->region_writer, storage);

	// start new meta data
	meta_writer_reset(&self->meta_writer);
	meta_writer_start(&self->meta_writer, opt_int_of(&config()->storage_crc));
}

void
writer_stop(Writer* self)
{
	if (! meta_writer_started(&self->meta_writer))
		return;

	// complete last region and meta data
	if (region_writer_started(&self->region_writer))
		writer_stop_region(self);
	meta_writer_stop(&self->meta_writer);

	// write meta
	auto encoder = &self->meta_writer.encoder;
	auto iov = encoder_iov(encoder);
	auto iov_count = encoder_iov_count(encoder);
	file_writev(self->file, iov, iov_count);

	// write meta footer
	file_write(self->file, &self->meta_writer.meta, sizeof(Meta));
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
