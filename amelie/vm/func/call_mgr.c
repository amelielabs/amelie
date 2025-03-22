
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>

void
call_mgr_init(CallMgr* self)
{
	self->local = NULL;
	self->data  = NULL;
	self->db    = NULL;
	buf_init(&self->context);
}

void
call_mgr_free(CallMgr* self)
{
	buf_free(&self->context);
}

void
call_mgr_prepare(CallMgr* self, Db* db, Local* local, CodeData* data)
{
	self->local = local;
	self->data  = data;
	self->db    = db;
	auto count = code_data_count_call(data);
	if (count == 0)
		return;
	buf_claim(&self->context, count * sizeof(void*));
	memset(self->context.start, 0, buf_size(&self->context));
}

void
call_mgr_reset(CallMgr* self)
{
	if (buf_empty(&self->context))
		return;

	auto count = code_data_count_call(self->data);
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
			.type     = CALL_CLEANUP,
			.function = code_data_at_call(self->data, i),
			.mgr      = self,
			.context  = context
		};
		cleanup_call.function->function(&cleanup_call);
		*context = NULL;
	}

	buf_reset(&self->context);
}
