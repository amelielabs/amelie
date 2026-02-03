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
	Buf     meta;
	Buf     data;
	Encoder encoder;
};

static inline void
region_writer_init(RegionWriter* self)
{
	buf_init(&self->meta);
	buf_init(&self->data);
	encoder_init(&self->encoder);
}

static inline void
region_writer_free(RegionWriter* self)
{
	buf_free(&self->meta);
	buf_free(&self->data);
	encoder_free(&self->encoder);
}

static inline void
region_writer_reset(RegionWriter* self)
{
	buf_reset(&self->meta);
	buf_reset(&self->data);
	encoder_reset(&self->encoder);
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
region_writer_open(RegionWriter* self, Tier* tier)
{
	encoder_open(&self->encoder, tier);
}

static inline void
region_writer_start(RegionWriter* self)
{
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

	// compress
	auto encoder = &self->encoder;
	encoder_add_buf(encoder, &self->meta);
	encoder_add_buf(encoder, &self->data);
	encoder_encode(encoder);
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

static inline uint32_t
region_writer_crc(RegionWriter* self)
{
	return encoder_iov_crc(&self->encoder);
}
