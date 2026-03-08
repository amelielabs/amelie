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

typedef struct Partitioning Partitioning;

struct Partitioning
{
	int64_t   partitions;
	bool      cache;
	int64_t   cache_size;
	int64_t   cache_evict;
	int64_t   cache_evict_wm;
	int64_t   cache_evict_min;
	VolumeMgr volumes;
};

static inline void
partitioning_init(Partitioning* self)
{
	self->partitions      = 0;
	self->cache           = false;
	self->cache_size      = 4ull * 1024 * 1024 * 1024;
	self->cache_evict     = 40;
	self->cache_evict_wm  = 90;
	self->cache_evict_min = 16ull * 1024 * 1024;
	volume_mgr_init(&self->volumes);
}

static inline void
partitioning_free(Partitioning* self)
{
	volume_mgr_free(&self->volumes);
}

static inline void
partitioning_set_partitions(Partitioning* self, int value)
{
	self->partitions = value;
}

static inline void
partitioning_set_cache(Partitioning* self, bool value)
{
	self->cache = value;
}

static inline void
partitioning_set_cache_size(Partitioning* self, int64_t value)
{
	self->cache_size = value;
}

static inline void
partitioning_set_cache_evict(Partitioning* self, int value)
{
	self->cache_evict = value;
}

static inline void
partitioning_set_cache_evict_wm(Partitioning* self, int value)
{
	self->cache_evict_wm = value;
}

static inline void
partitioning_set_cache_evict_min(Partitioning* self, int value)
{
	self->cache_evict_min = value;
}

static inline void
partitioning_copy(Partitioning* self, Partitioning* copy)
{
	partitioning_set_partitions(copy, self->partitions);
	partitioning_set_cache(copy, self->cache);
	partitioning_set_cache_size(copy, self->cache_size);
	partitioning_set_cache_evict(copy, self->cache_evict);
	partitioning_set_cache_evict_wm(copy, self->cache_evict_wm);
	partitioning_set_cache_evict_min(copy, self->cache_evict_min);
	volume_mgr_copy(&self->volumes, &copy->volumes);
}

static inline void
partitioning_read(Partitioning* self, uint8_t** pos)
{
	uint8_t* pos_volumes = NULL;
	Decode obj[] =
	{
		{ DECODE_INT,   "partitions",      &self->partitions      },
		{ DECODE_BOOL,  "cache",           &self->cache           },
		{ DECODE_INT,   "cache_size",      &self->cache_size      },
		{ DECODE_INT,   "cache_evict",     &self->cache_evict     },
		{ DECODE_INT,   "cache_evict_wm",  &self->cache_evict_wm  },
		{ DECODE_INT,   "cache_evict_min", &self->cache_evict_min },
		{ DECODE_ARRAY, "volumes",         &pos_volumes           },
		{ 0,             NULL,              NULL                  },
	};
	decode_obj(obj, "partitioning", pos);

	// volumes
	volume_mgr_read(&self->volumes, &pos_volumes);
}

static inline void
partitioning_write(Partitioning* self, Buf* buf, int flags)
{
	encode_obj(buf);

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_integer(buf, self->partitions);

	// cache
	encode_raw(buf, "cache", 5);
	encode_bool(buf, self->cache);

	// cache_size
	encode_raw(buf, "cache_size", 10);
	encode_integer(buf, self->cache_size);

	// cache_evict
	encode_raw(buf, "cache_evict", 11);
	encode_integer(buf, self->cache_evict);

	// cache_evict_wm
	encode_raw(buf, "cache_evict_wm", 14);
	encode_integer(buf, self->cache_evict_wm);

	// cache_evict_min
	encode_raw(buf, "cache_evict_min", 15);
	encode_integer(buf, self->cache_evict_min);

	// volumes
	encode_raw(buf, "volumes", 7);
	volume_mgr_write(&self->volumes, buf, flags);

	encode_obj_end(buf);
}
