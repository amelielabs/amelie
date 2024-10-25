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

typedef struct BufMgr BufMgr;

struct BufMgr
{
	atomic_u64 seq;
	BufCache   cache[8];
};

static inline void
buf_mgr_init(BufMgr* self)
{
	self->seq = 0;
	for (int i = 0; i < 8; i++)
		buf_cache_init(&self->cache[i], 32 * 1024);
}

static inline void
buf_mgr_free(BufMgr* self)
{
	self->seq = 0;
	for (int i = 0; i < 8; i++)
		buf_cache_free(&self->cache[i]);
}

static inline Buf*
buf_mgr_create(BufMgr* self, int size)
{
	int next = atomic_u64_inc(&self->seq) % 8;
	return buf_cache_create(&self->cache[next], size);
}
