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

typedef struct SpanWriter SpanWriter;

struct SpanWriter
{
	bool   active;
	Buf    data;
	Buf    compressed;
	Codec* compression;
	Buf    encrypted;
	Codec* encryption;
	bool   crc;
	Span   span;
};

static inline void
span_writer_init(SpanWriter* self)
{
	self->active      = false;
	self->compression = NULL;
	self->encryption  = NULL;
	self->crc         = false;
	buf_init(&self->data);
	buf_init(&self->compressed);
	buf_init(&self->encrypted);
	memset(&self->span, 0, sizeof(self->span));
}

static inline void
span_writer_free(SpanWriter* self)
{
	buf_free(&self->data);
	buf_free(&self->compressed);
	buf_free(&self->encrypted);
}

static inline void
span_writer_reset(SpanWriter* self)
{
	self->active      = false;
	self->compression = NULL;
	self->encryption  = NULL;
	self->crc         = false;
	buf_reset(&self->data);
	buf_reset(&self->compressed);
	buf_reset(&self->encrypted);
	memset(&self->span, 0, sizeof(self->span));
}

static inline bool
span_writer_started(SpanWriter* self)
{
	return self->active;
}

static inline void
span_writer_start(SpanWriter* self,
                  Codec*      compression,
                  Codec*      encryption,
                  bool        crc)
{
	self->compression = compression;
	self->encryption  = encryption;
	self->crc         = crc;
	self->active      = true;
}

static inline void
span_writer_stop(SpanWriter* self,
                  Id*        id,
                  uint32_t   refreshes,
                  uint64_t   time_create,
                  uint64_t   time_refresh,
                  uint64_t   lsn)
{
	auto span = &self->span;

	// compress span data (without span header)
	uint32_t size_origin = span->size;
	uint32_t size = size_origin;
	int      compression_id;
	if (self->compression)
	{
		auto codec = self->compression;
		auto buf = &self->compressed;
		codec_encode_begin(codec, buf);
		codec_encode_buf(codec, buf, &self->data);
		codec_encode_end(codec, buf);

		size = buf_size(&self->compressed);
		compression_id = self->compression->iface->id;
	} else {
		compression_id = COMPRESSION_NONE;
	}

	// encrypt span data
	int encryption_id;
	if (self->encryption)
	{
		auto codec = self->encryption;
		auto buf = &self->encrypted;
		codec_encode_begin(codec, buf);
		if (self->compression)
			codec_encode_buf(codec, buf, &self->compressed);
		else
			codec_encode_buf(codec, buf, &self->data);
		codec_encode_end(codec, buf);

		size = buf_size(&self->encrypted);
		encryption_id = self->encryption->iface->id;
	} else
	{
		encryption_id = CIPHER_NONE;
	}

	// prepare span
	span->crc               = 0;
	span->crc_data          = 0;
	span->magic             = SPAN_MAGIC;
	span->version           = SPAN_VERSION;
	span->id                = *id;
	span->size              = size;
	span->size_origin       = size_origin;
	span->size_total        =
		span->size_regions + span->size + sizeof(Span);
	span->size_total_origin =
		span->size_regions_origin + span->size_origin + sizeof(Span);
	span->time_create       = time_create;
	span->time_refresh      = time_refresh;
	span->refreshes         = refreshes;
	span->lsn               = lsn;
	span->compression       = compression_id;
	span->encryption        = encryption_id;

	// calculate span data crc
	uint32_t crc = 0;
	if (self->encryption)
		crc = runtime()->crc(crc, self->encrypted.start, buf_size(&self->encrypted));
	else
	if (self->compression)
		crc = runtime()->crc(crc, self->compressed.start, buf_size(&self->compressed));
	else
		crc = runtime()->crc(crc, self->data.start, buf_size(&self->data));
	span->crc_data = crc;

	// calculate span crc
	span->crc = runtime()->crc(0, span, sizeof(Span));
}

static inline void
span_writer_add(SpanWriter*   self,
                RegionWriter* region_writer,
                uint64_t      region_offset)
{
	auto region = region_writer_header(region_writer);
	assert(region->rows > 0);

	uint32_t size;
	if (self->encryption)
		size = buf_size(&region_writer->encrypted);
	else
	if (self->compression)
		size = buf_size(&region_writer->compressed);
	else
		size = region->size;

	uint32_t crc = 0;
	if (self->crc)
		crc = region_writer_crc(region_writer);

	// prepare meta region reference
	buf_reserve(&self->data, sizeof(SpanRegion));
	auto min      = region_writer_min(region_writer);
	auto min_size = row_size(min);
	auto max      = region_writer_max(region_writer);
	auto max_size = row_size(max);

	auto ref = (SpanRegion*)self->data.position;
	ref->offset      = region_offset;
	ref->crc         = crc;
	ref->size        = size;
	ref->size_origin = region->size;
	ref->size_min    = min_size;
	ref->size_max    = max_size;
	ref->rows        = region->rows;
	memset(ref->reserved, 0, sizeof(ref->reserved));
	buf_write(&self->data, min, min_size);
	buf_write(&self->data, max, max_size);
	buf_advance(&self->data, sizeof(SpanRegion));

	// update header
	auto span = &self->span;
	span->regions++;
	span->rows += region->rows;
	span->size += sizeof(SpanRegion);
	span->size_regions += size;
	span->size_regions_origin += region->size;
}

static inline void
span_writer_copy(SpanWriter* self, Span* span, Buf* buf)
{
	*span = self->span;
	buf_write(buf, self->data.start, buf_size(&self->data));
}

static inline void
span_writer_add_to_iov(SpanWriter* self, Iov* iov)
{
	if (self->encryption)
		iov_add_buf(iov, &self->encrypted);
	else
	if (self->compression)
		iov_add_buf(iov, &self->compressed);
	else
		iov_add_buf(iov, &self->data);
	iov_add(iov, &self->span, sizeof(self->span));
}
