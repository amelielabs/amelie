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

typedef struct MetaWriter MetaWriter;

struct MetaWriter
{
	bool   active;
	Buf    data;
	Buf    compressed;
	Codec* compression;
	Buf    encrypted;
	Codec* encryption;
	bool   crc;
	Meta   meta;
};

static inline void
meta_writer_init(MetaWriter* self)
{
	self->active      = false;
	self->compression = NULL;
	self->encryption  = NULL;
	self->crc         = false;
	buf_init(&self->data);
	buf_init(&self->compressed);
	buf_init(&self->encrypted);
	meta_init(&self->meta);
}

static inline void
meta_writer_free(MetaWriter* self)
{
	buf_free(&self->data);
	buf_free(&self->compressed);
	buf_free(&self->encrypted);
}

static inline void
meta_writer_reset(MetaWriter* self)
{
	self->active      = false;
	self->compression = NULL;
	self->encryption  = NULL;
	self->crc         = false;
	buf_reset(&self->data);
	buf_reset(&self->compressed);
	buf_reset(&self->encrypted);
	meta_init(&self->meta);
}

static inline bool
meta_writer_started(MetaWriter* self)
{
	return self->active;
}

static inline void
meta_writer_start(MetaWriter* self,
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
meta_writer_stop(MetaWriter* self,
                 Id*         id,
                 uint32_t    refreshes,
                 uint64_t    time_create,
                 uint64_t    time_refresh,
                 uint64_t    lsn)
{
	auto meta = &self->meta;

	// compress meta data (without header)
	uint32_t size_origin = meta->size;
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

	// encrypt meta data
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

	// prepare header
	meta->crc               = 0;
	meta->crc_data          = 0;
	meta->magic             = META_MAGIC;
	meta->version           = META_VERSION;
	meta->id                = *id;
	meta->size              = size;
	meta->size_origin       = size_origin;
	meta->size_total        =
		meta->size_regions + meta->size + sizeof(Meta);
	meta->size_total_origin =
		meta->size_regions_origin + meta->size_origin + sizeof(Meta);
	meta->time_create       = time_create;
	meta->time_refresh      = time_refresh;
	meta->refreshes         = refreshes;
	meta->lsn               = lsn;
	meta->compression       = compression_id;
	meta->encryption        = encryption_id;

	// calculate data crc
	uint32_t crc = 0;
	if (self->encryption)
		crc = runtime()->crc(crc, self->encrypted.start, buf_size(&self->encrypted));
	else
	if (self->compression)
		crc = runtime()->crc(crc, self->compressed.start, buf_size(&self->compressed));
	else
		crc = runtime()->crc(crc, self->data.start, buf_size(&self->data));
	meta->crc_data = crc;

	// calculate header crc
	meta->crc = runtime()->crc(0, meta, sizeof(Meta));
}

static inline void
meta_writer_add(MetaWriter*   self,
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

	// prepare region reference
	buf_reserve(&self->data, sizeof(MetaRegion));
	auto min      = region_writer_min(region_writer);
	auto min_size = row_size(min);
	auto max      = region_writer_max(region_writer);
	auto max_size = row_size(max);

	auto ref = (MetaRegion*)self->data.position;
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
	buf_advance(&self->data, sizeof(MetaRegion));

	// update header
	auto meta = &self->meta;
	meta->regions++;
	meta->rows += region->rows;
	meta->size += sizeof(MetaRegion);
	meta->size_regions += size;
	meta->size_regions_origin += region->size;
}

static inline void
meta_writer_copy(MetaWriter* self, Meta* meta, Buf* data)
{
	*meta = self->meta;
	buf_write(data, self->data.start, buf_size(&self->data));
}

static inline void
meta_writer_add_to_iov(MetaWriter* self, Iov* iov)
{
	if (self->encryption)
		iov_add_buf(iov, &self->encrypted);
	else
	if (self->compression)
		iov_add_buf(iov, &self->compressed);
	else
		iov_add_buf(iov, &self->data);
	iov_add(iov, &self->meta, sizeof(self->meta));
}
