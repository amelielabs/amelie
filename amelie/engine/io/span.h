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

typedef struct SpanRegion SpanRegion;
typedef struct Span       Span;

#define SPAN_MAGIC   0x20849615
#define SPAN_VERSION 0

struct SpanRegion
{
	uint32_t offset;
	uint32_t crc;
	uint32_t size;
	uint32_t size_origin;
	uint32_t size_min;
	uint32_t size_max;
	uint32_t rows;
	uint32_t reserved[4];
	uint8_t  data[];
} packed;

struct Span
{
	uint32_t crc;
	uint32_t crc_data;
	uint32_t magic;
	uint32_t version;
	Id       id;
	uint32_t size;
	uint32_t size_origin;
	uint64_t size_regions;
	uint64_t size_regions_origin;
	uint64_t size_total;
	uint64_t size_total_origin;
	uint32_t regions;
	uint64_t rows;
	uint64_t time_create;
	uint64_t time_refresh;
	uint32_t refreshes;
	uint64_t lsn;
	uint8_t  compression;
	uint8_t  encryption;
	uint32_t reserved[4];
} packed;

static inline void
span_init(Span* self)
{
	memset(self, 0, sizeof(*self));
}

static inline SpanRegion*
span_region(Span* self, Buf* data, int pos)
{
	unused(self);
	assert(pos < (int)self->regions);
	return &((SpanRegion*)data->start)[pos];
}

static inline Row*
span_region_min(Span* self, Buf* data, int pos)
{
	return (Row*)span_region(self, data, pos)->data;
}

static inline Row*
span_region_max(Span* self, Buf* data, int pos)
{
	auto region = span_region(self, data, pos);
	return (Row*)(region->data + region->size_min);
}

static inline Row*
span_min(Span* self, Buf* data)
{
	return span_region_min(self, data, 0);
}

static inline Row*
span_max(Span* self, Buf* data)
{
	return span_region_max(self, data, self->regions - 1);
}
