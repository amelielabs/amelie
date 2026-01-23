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
	Buf     offsets;
	Buf     data;
	Meta    meta;
	bool    active;
	bool    crc;
	Encoder encoder;
};

static inline void
meta_writer_init(MetaWriter* self)
{
	self->active  = false;
	self->crc     = false;
	buf_init(&self->offsets);
	buf_init(&self->data);
	meta_init(&self->meta);
	encoder_init(&self->encoder);
}

static inline void
meta_writer_free(MetaWriter* self)
{
	buf_free(&self->offsets);
	buf_free(&self->data);
	encoder_free(&self->encoder);
}

static inline void
meta_writer_reset(MetaWriter* self)
{
	self->active  = false;
	self->crc     = false;
	buf_reset(&self->offsets);
	buf_reset(&self->data);
	meta_init(&self->meta);
	encoder_reset(&self->encoder);
}

static inline bool
meta_writer_started(MetaWriter* self)
{
	return self->active;
}

static inline void
meta_writer_open(MetaWriter* self, Encoding* encoding)
{
	encoder_open(&self->encoder, encoding);
}

static inline void
meta_writer_start(MetaWriter* self, bool crc)
{
	self->active = true;
	self->crc    = crc;
}

static inline void
meta_writer_stop(MetaWriter* self, Id* id)
{
	// [[offsets][meta_regions]] [meta]
	auto meta = &self->meta;

	// compress and encrypt meta data (without header)
	auto encoder = &self->encoder;
	encoder_add_buf(encoder, &self->offsets);
	encoder_add_buf(encoder, &self->data);
	encoder_encode(encoder);

	// prepare header
	meta->crc               = 0;
	meta->crc_data          = 0;
	meta->magic             = META_MAGIC;
	meta->version           = META_VERSION;
	meta->id                = *id;
	meta->size_origin       = meta->size;
	meta->size              = encoder_iov(encoder)->iov_len;
	meta->size_total        =
		meta->size_regions + meta->size + sizeof(Meta);
	meta->size_total_origin =
		meta->size_regions_origin + meta->size_origin + sizeof(Meta);
	meta->compression       = encoder_compression(encoder);
	meta->encryption        = encoder_encryption(encoder);
	meta->crc_data          = encoder_iov_crc(encoder);
	meta->crc               = runtime()->crc(0, meta, sizeof(Meta));
}

static inline void
meta_writer_add(MetaWriter*   self,
                RegionWriter* region_writer,
                uint64_t      region_offset)
{
	auto region = region_writer_header(region_writer);
	assert(region->rows > 0);

	// get region size
	uint32_t size = encoder_iov(&region_writer->encoder)->iov_len;

	// calculate region crc
	uint32_t crc = 0;
	if (self->crc)
		crc = region_writer_crc(region_writer);

	// add region reference offset
	uint32_t offset = buf_size(&self->data);
	buf_write(&self->offsets, &offset, sizeof(offset));

	// add region reference
	auto min      = region_writer_min(region_writer);
	auto min_size = row_size(min);
	auto max      = region_writer_max(region_writer);
	auto max_size = row_size(max);

	auto ref = (MetaRegion*)buf_emplace(&self->data, sizeof(MetaRegion));
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

	// update header
	auto meta = &self->meta;
	meta->regions++;
	meta->rows += region->rows;
	meta->size += sizeof(uint32_t) + sizeof(MetaRegion) + min_size + max_size;
	meta->size_regions += size;
	meta->size_regions_origin += region->size;
}

static inline void
meta_writer_copy(MetaWriter* self, Meta* meta, Buf* data)
{
	*meta = self->meta;
	buf_write_buf(data, &self->offsets);
	buf_write_buf(data, &self->data);
}

hot static inline uint64_t
meta_writer_total(MetaWriter* self)
{
	if (unlikely(! meta_writer_started(self)))
		return 0;
	// total compressed size (actual file size)
	auto meta = &self->meta;
	return meta->size_regions + meta->size + sizeof(Meta);
}
