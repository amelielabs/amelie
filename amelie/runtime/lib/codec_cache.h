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

typedef struct CodecCache CodecCache;

struct CodecCache
{
	Spinlock lock;
	List     list;
};

static inline void
codec_cache_init(CodecCache* self)
{
	spinlock_init(&self->lock);
	list_init(&self->list);
}

static inline void
codec_cache_free(CodecCache* self)
{
	list_foreach_safe(&self->list)
	{
		auto ref = list_at(Codec, link);
		codec_free(ref);
	}
	spinlock_free(&self->lock);
}

static inline void
codec_cache_push(CodecCache* self, Codec* cp)
{
	spinlock_lock(&self->lock);
	list_append(&self->list, &cp->link);
	spinlock_unlock(&self->lock);
}

static inline Codec*
codec_cache_pop(CodecCache* self, int id)
{
	Codec* codec = NULL;
	spinlock_lock(&self->lock);
	if (! list_empty(&self->list))
	{
		list_foreach_safe(&self->list)
		{
			auto ref = list_at(Codec, link);
			if (ref->iface->id == id)
			{
				list_unlink(&ref->link);
				codec = container_of(ref, Codec, link);
				break;
			}
		}
	}
	spinlock_unlock(&self->lock);
	return codec;
}
