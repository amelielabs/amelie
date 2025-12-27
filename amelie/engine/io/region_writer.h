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

typedef struct RegionWriter RegionWriter;

struct RegionWriter
{
	Buf          meta;
	Buf          data;
	Buf          compressed;
	Buf          encrypted;
	Compression* compression;
	int          compression_level;
	Encryption*  encryption;
	Str*         encryption_key;
};

static inline void
region_writer_init(RegionWriter* self)
{
	self->compression       = NULL;
	self->compression_level = 0;
	self->encryption        = NULL;
	self->encryption_key    = NULL;
	buf_init(&self->meta);
	buf_init(&self->data);
	buf_init(&self->compressed);
	buf_init(&self->encrypted);
}

static inline void
region_writer_free(RegionWriter* self)
{
	buf_free(&self->meta);
	buf_free(&self->data);
	buf_free(&self->compressed);
	buf_free(&self->encrypted);
}

static inline void
region_writer_reset(RegionWriter* self)
{
	self->compression       = NULL;
	self->compression_level = 0;
	self->encryption        = NULL;
	self->encryption_key    = NULL;
	buf_reset(&self->meta);
	buf_reset(&self->data);
	buf_reset(&self->compressed);
	buf_reset(&self->encrypted);
}

static inline Region*
region_writer_header(RegionWriter* self)
{
	assert(self->meta.start != NULL);
	return (Region*)self->meta.start;
}

static inline bool
region_writer_started(RegionWriter* self)
{
	return buf_size(&self->meta) > 0;
}

static inline uint32_t
region_writer_size(RegionWriter* self)
{
	return buf_size(&self->meta) + buf_size(&self->data);
}

static inline void
region_writer_start(RegionWriter* self,
                    Compression*  compression,
                    int           compression_level,
                    Encryption*   encryption,
                    Str*          encryption_key)
{
	self->compression       = compression;
	self->compression_level = compression_level;
	self->encryption        = encryption;
	self->encryption_key    = encryption_key;

	// region header
	buf_reserve(&self->meta, sizeof(Region));
	auto header = region_writer_header(self);
	memset(header, 0, sizeof(*header));
	buf_advance(&self->meta, sizeof(Region));
}

static inline void
region_writer_stop(RegionWriter* self)
{
	auto header = region_writer_header(self);
	header->size = region_writer_size(self);

	// compress region
	if (self->compression)
	{
		auto cp  = self->compression;
		auto buf = &self->compressed;
		compression_begin(cp, self->compression_level);
		compression_add_buf(cp, buf, &self->meta);
		compression_add_buf(cp, buf, &self->data);
		compression_end(cp, buf);
	}

	// encrypt region using compressed or raw data
	if (self->encryption)
	{
		auto ec  = self->encryption;
		auto buf = &self->encrypted;
		encryption_begin(ec, &runtime()->random, self->encryption_key, buf);
		if (self->compression)
		{
			encryption_add_buf(ec, buf, &self->compressed);
		} else
		{
			encryption_add_buf(ec, buf, &self->meta);
			encryption_add_buf(ec, buf, &self->data);
		}
		encryption_end(ec, buf);
	}
}

static inline void
region_writer_add(RegionWriter* self, Row* row)
{
	// write data offset
	uint32_t offset = buf_size(&self->data);
	buf_write(&self->meta, &offset, sizeof(offset));

	// copy data
	uint32_t size = row_size(row);
	buf_write(&self->data, row, size);

	// update header
	auto header = region_writer_header(self);
	header->rows++;
}

static inline Row*
region_writer_min(RegionWriter* self)
{
	return (Row*)self->data.start;
}

static inline Row*
region_writer_max(RegionWriter* self)
{
	auto header = region_writer_header(self);
	assert(header->rows > 0);
	auto offset = (uint32_t*)(self->meta.start + sizeof(Region));
	return (Row*)(self->data.start + offset[header->rows - 1]);
}

static inline Row*
region_writer_last(RegionWriter* self)
{
	if (region_writer_header(self)->rows == 0)
		return NULL;
	return region_writer_max(self);
}

static inline void
region_writer_add_to_iov(RegionWriter* self, Iov* iov)
{
	if (self->encryption)
	{
		iov_add_buf(iov, &self->encrypted);
	} else
	if (self->compression)
	{
		iov_add_buf(iov, &self->compressed);
	} else
	{
		iov_add_buf(iov, &self->meta);
		iov_add_buf(iov, &self->data);
	}
}

static inline uint32_t
region_writer_crc(RegionWriter* self)
{
	uint32_t crc = 0;
	if (self->encryption)
	{
		crc = runtime()->crc(crc, self->encrypted.start, buf_size(&self->encrypted));
	} else
	if (self->compression)
	{
		crc = runtime()->crc(crc, self->compressed.start, buf_size(&self->compressed));
	} else
	{
		crc = runtime()->crc(crc, self->meta.start, buf_size(&self->meta));
		crc = runtime()->crc(crc, self->data.start, buf_size(&self->data));
	}
	return crc;
}
