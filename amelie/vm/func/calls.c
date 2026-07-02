
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
#include <amelie_db>
#include <amelie_sync>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_output.h>
#include <amelie_commit.h>
#include <amelie_func.h>

void
calls_init(Calls* self)
{
	self->local = NULL;
	self->data  = NULL;
	buf_init(&self->context);
}

void
calls_free(Calls* self)
{
	buf_free(&self->context);
}

void
calls_prepare(Calls* self, Local* local, CodeData* data)
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
calls_reset(Calls* self)
{
	if (buf_empty(&self->context))
		return;

	auto count = code_data_count_fn(self->data);
	for (int i = 0; i < count; i++)
	{
		auto context = calls_at(self, i);
		if (! *context)
			continue;
		Call cleanup =
		{
			.argc     = 0,
			.argv     = NULL,
			.result   = NULL,
			.type     = CALL_CLEANUP,
			.function = code_data_at_fn(self->data, i),
			.context  = context
		};
		cleanup.function->function(&cleanup);
		*context = NULL;
	}

	buf_reset(&self->context);
}
