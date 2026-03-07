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

typedef struct PartMgrConfig PartMgrConfig;

struct PartMgrConfig
{
	int64_t   partitions;
	bool      cache;
	int64_t   cache_size;
	int64_t   cache_evict;
	int64_t   cache_evict_wm;
	VolumeMgr volumes;
};

static inline void
part_mgr_config_init(PartMgrConfig* self)
{
	self->partitions     = 0;
	self->cache          = false;
	self->cache_size     = 4ull * 1024 * 1024 * 1024;
	self->cache_evict    = 40;
	self->cache_evict_wm = 90;
	volume_mgr_init(&self->volumes);
}

static inline void
part_mgr_config_free(PartMgrConfig* self)
{
	volume_mgr_free(&self->volumes);
}

static inline void
part_mgr_config_set_partitions(PartMgrConfig* self, int value)
{
	self->partitions = value;
}

static inline void
part_mgr_config_set_cache(PartMgrConfig* self, bool value)
{
	self->cache = value;
}

static inline void
part_mgr_config_set_cache_size(PartMgrConfig* self, int64_t value)
{
	self->cache_size = value;
}

static inline void
part_mgr_config_set_cache_evict(PartMgrConfig* self, int value)
{
	self->cache_evict = value;
}

static inline void
part_mgr_config_set_cache_evict_wm(PartMgrConfig* self, int value)
{
	self->cache_evict_wm = value;
}

static inline void
part_mgr_config_copy(PartMgrConfig* self, PartMgrConfig* copy)
{
	part_mgr_config_set_partitions(copy, self->partitions);
	part_mgr_config_set_cache(copy, self->cache);
	part_mgr_config_set_cache_size(copy, self->cache_size);
	part_mgr_config_set_cache_evict(copy, self->cache_evict);
	part_mgr_config_set_cache_evict_wm(copy, self->cache_evict_wm);
	volume_mgr_copy(&self->volumes, &copy->volumes);
}

static inline void
part_mgr_config_read(PartMgrConfig* self, uint8_t** pos)
{
	uint8_t* pos_volumes = NULL;
	Decode obj[] =
	{
		{ DECODE_INT,   "partitions",     &self->partitions     },
		{ DECODE_BOOL,  "cache",          &self->cache          },
		{ DECODE_INT,   "cache_size",     &self->cache_size     },
		{ DECODE_INT,   "cache_evict",    &self->cache_evict    },
		{ DECODE_INT,   "cache_evict_wm", &self->cache_evict_wm },
		{ DECODE_ARRAY, "volumes",        &pos_volumes          },
		{ 0,             NULL,             NULL                 },
	};
	decode_obj(obj, "part_mgr_config", pos);

	// volumes
	volume_mgr_read(&self->volumes, &pos_volumes);
}

static inline void
part_mgr_config_write(PartMgrConfig* self, Buf* buf, int flags)
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

	// volumes
	encode_raw(buf, "volumes", 7);
	volume_mgr_write(&self->volumes, buf, flags);

	encode_obj_end(buf);
}
