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

typedef struct VolumeMgr VolumeMgr;

struct VolumeMgr
{
	List list;
	int  list_count;
};

static inline void
volume_mgr_init(VolumeMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
volume_mgr_free(VolumeMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto volume = list_at(Volume, link);
		volume_free(volume);
	}
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
volume_mgr_add(VolumeMgr* self, Volume* volume)
{
	list_append(&self->list, &volume->link);
	self->list_count++;
}

static inline void
volume_mgr_remove(VolumeMgr* self, Volume* volume)
{
	list_unlink(&volume->link);
	self->list_count--;
}

static inline void
volume_mgr_copy(VolumeMgr* self, VolumeMgr* copy)
{
	list_foreach(&self->list)
	{
		auto volume = list_at(Volume, link);
		auto volume_copy_ref = volume_copy(volume);
		volume_mgr_add(copy, volume_copy_ref);
	}
}

static inline void
volume_mgr_read(VolumeMgr* self, uint8_t** pos)
{
	json_read_array(pos);
	while (! json_read_array_end(pos))
	{
		auto volume = volume_read(pos);
		volume_mgr_add(self, volume);
	}
}

static inline void
volume_mgr_write(VolumeMgr* self, Buf* buf, int flags)
{
	encode_array(buf);
	list_foreach(&self->list)
	{
		auto volume = list_at(Volume, link);
		volume_write(volume, buf, flags);
	}
	encode_array_end(buf);
}

static inline void
volume_mgr_ref(VolumeMgr* self, StorageMgr* storage_mgr)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Volume, link);
		ref->storage = storage_mgr_find(storage_mgr, &ref->name, true);
		storage_ref(ref->storage);
	}
}

static inline void
volume_mgr_unref(VolumeMgr* self)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Volume, link);
		if (ref->storage)
		{
			storage_unref(ref->storage);
			ref->storage = NULL;
		}
	}
}

static inline Volume*
volume_mgr_find(VolumeMgr* self, Str* name)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Volume, link);
		if (str_compare(&ref->name, name))
			return ref;
	}
	return NULL;
}

static inline bool
volume_mgr_has(VolumeMgr* self, Volume* volume)
{
	list_foreach(&self->list)
	{
		auto ref = list_at(Volume, link);
		if (ref == volume)
			return true;
	}
	return false;
}

static inline Volume*
volume_mgr_next(VolumeMgr* self)
{
	Volume* volume = NULL;
	list_foreach(&self->list)
	{
		auto ref = list_at(Volume, link);
		if (ref->pause)
			continue;
		if (!volume || ref->refs < volume->refs)
			volume = ref;
	}
	return volume;
}

static inline int
volume_mgr_count(VolumeMgr* self)
{
	int count = 0;
	list_foreach(&self->list)
	{
		auto ref = list_at(Volume, link);
		if (ref->pause)
			continue;
		count++;
	}
	return count;
}

static inline Volume*
volume_mgr_first(VolumeMgr* self)
{
	return container_of(list_first(&self->list), Volume, link);
}

static inline void
volume_mgr_mkdir(VolumeMgr* self)
{
	list_foreach(&self->list)
	{
		auto volume = list_at(Volume, link);
		volume_mkdir(volume);
	}
}

static inline void
volume_mgr_rmdir(VolumeMgr* self)
{
	list_foreach(&self->list)
	{
		auto volume = list_at(Volume, link);
		volume_rmdir(volume);
	}
}

static inline void
volume_mgr_list(VolumeMgr* self, Buf* buf)
{
	list_foreach(&self->list)
	{
		auto volume = list_at(Volume, link);
		char id[UUID_SZ];
		uuid_get(&volume->id, id, sizeof(id));
		char path[256];
		auto path_size = sfmt(path, sizeof(path), "storage/%s", id);
		encode_raw(buf, path, path_size);
	}
}
