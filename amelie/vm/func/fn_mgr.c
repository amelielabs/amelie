
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_engine>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_executor.h>
#include <amelie_func.h>

void
fn_mgr_init(FnMgr* self)
{
	self->local = NULL;
	self->data  = NULL;
	buf_init(&self->context);
}

void
fn_mgr_free(FnMgr* self)
{
	buf_free(&self->context);
}

void
fn_mgr_prepare(FnMgr* self, Local* local, CodeData* data)
{
	self->local = local;
	self->data  = data;
	auto count = code_data_count_fn(data);
	if (count == 0)
		return;
	buf_emplace(&self->context, count * sizeof(void*));
	memset(self->context.start, 0, buf_size(&self->context));
}

void
fn_mgr_reset(FnMgr* self)
{
	if (buf_empty(&self->context))
		return;

	auto count = code_data_count_fn(self->data);
	for (int i = 0; i < count; i++)
	{
		auto context = fn_mgr_at(self, i);
		if (! *context)
			continue;
		Fn cleanup_fn =
		{
			.argc     = 0,
			.argv     = NULL,
			.result   = NULL,
			.action   = FN_CLEANUP,
			.function = code_data_at_fn(self->data, i),
			.context  = context
		};
		cleanup_fn.function->function(&cleanup_fn);
		*context = NULL;
	}

	buf_reset(&self->context);
}
