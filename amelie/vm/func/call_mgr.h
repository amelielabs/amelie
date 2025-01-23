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

typedef struct CallMgr CallMgr;

struct CallMgr
{
	Local*    local;
	CodeData* data;
	Db*       db;
	Buf       context;
};

void call_mgr_init(CallMgr*);
void call_mgr_free(CallMgr*);
void call_mgr_prepare(CallMgr*, Db*, Local*, CodeData*);
void call_mgr_reset(CallMgr*);

static inline void**
call_mgr_at(CallMgr* self, int id)
{
	assert(buf_size(&self->context) > 0);
	auto list = (void**)self->context.start;
	return &list[id];
}
