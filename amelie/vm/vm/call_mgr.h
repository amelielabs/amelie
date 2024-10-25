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
typedef struct Vm      Vm;

struct CallMgr
{
	Buf context;
};

static inline void
call_mgr_init(CallMgr* self)
{
	buf_init(&self->context);
}

static inline void
call_mgr_free(CallMgr* self)
{
	buf_free(&self->context);
}

static inline void**
call_mgr_at(CallMgr* self, int id)
{
	assert(buf_size(&self->context) > 0);
	auto list = (void**)self->context.start;
	return &list[id];
}

static inline void
call_mgr_reset(CallMgr* self, CodeData* data, Vm* vm)
{
	if (! buf_size(&self->context))
		return;

	auto count = code_data_count_call(data);
	for (int i = 0; i < count; i++)
	{
		auto context = call_mgr_at(self, i);
		if (! *context)
			continue;
		Call cleanup_call =
		{
			.argc     = 0,
			.argv     = NULL,
			.result   = NULL,
			.vm       = vm,
			.type     = CALL_CLEANUP,
			.function = code_data_at_call(data, i),
			.context  = context
		};
		cleanup_call.function->main(&cleanup_call);
		*context = NULL;
	}

	buf_reset(&self->context);
}

static inline void
call_mgr_prepare(CallMgr* self, CodeData* data)
{
	auto count = code_data_count_call(data);
	if (count == 0)
		return;
	buf_claim(&self->context, count * sizeof(void*));
	memset(self->context.start, 0, buf_size(&self->context));
}
