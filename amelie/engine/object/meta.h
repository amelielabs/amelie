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

typedef struct MetaRegion MetaRegion;
typedef struct Meta       Meta;

#define META_MAGIC   0x20849615
#define META_VERSION 0

struct MetaRegion
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

struct Meta
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
meta_init(Meta* self)
{
	memset(self, 0, sizeof(*self));
}

static inline MetaRegion*
meta_region(Meta* self, Buf* data, int pos)
{
	unused(self);
	assert(pos < (int)self->regions);
	return &((MetaRegion*)data->start)[pos];
}

static inline MetaRegion*
meta_min(Meta* self, Buf* data)
{
	return meta_region(self, data, 0);
}

static inline MetaRegion*
meta_max(Meta* self, Buf* data)
{
	return meta_region(self, data, self->regions - 1);
}

static inline Row*
meta_region_min(MetaRegion* self)
{
	return (Row*)self->data;
}

static inline Row*
meta_region_max(MetaRegion* self)
{
	return (Row*)(self->data + self->size_min);
}
