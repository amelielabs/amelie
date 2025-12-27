
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_io.h>

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
		compression_free(self->compression);
		self->compression = NULL;
	}
	if (self->encryption)
	{
		encryption_free(self->encryption);
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
	                    self->source->compression_level,
	                    self->encryption,
	                    &self->source->encryption_key);
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

	// create compression context
	if (str_is(&source->compression, "zstd", 4))
		self->compression = compression_create(&compression_zstd);

	// create encryption context
	if (str_is(&source->compression, "aes", 3))
		self->encryption = encryption_create(&encryption_aes);

	// start new span
	span_writer_reset(&self->span_writer);
	span_writer_start(&self->span_writer, self->compression,
	                  source->compression_level,
	                  self->encryption,
	                  &source->encryption_key,
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
	                 time_create, time_refresh,
	                 lsn);

	// write span
	iov_reset(&self->iov);
	span_writer_add_to_iov(&self->span_writer, &self->iov);
	file_writev(self->file, iov_pointer(&self->iov), self->iov.iov_count);
	
	// sync
	if (sync)
		file_sync(self->file);

	// cleanup
	if (self->compression)
	{
		compression_free(self->compression);
		self->compression = NULL;
	}

	if (self->encryption)
	{
		encryption_free(self->encryption);
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
}
