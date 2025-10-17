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

typedef struct FnMgr FnMgr;

struct FnMgr
{
	Local*    local;
	CodeData* data;
	Buf       context;
};

void fn_mgr_init(FnMgr*);
void fn_mgr_free(FnMgr*);
void fn_mgr_prepare(FnMgr*, Local*, CodeData*);
void fn_mgr_reset(FnMgr*);

static inline void**
fn_mgr_at(FnMgr* self, int id)
{
	assert(buf_size(&self->context) > 0);
	auto list = (void**)self->context.start;
	return &list[id];
}
