
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>
#include <indigo_shard.h>
#include <indigo_hub.h>
#include <indigo_session.h>

static void
explain_portal(Portal* self, Buf* buf)
{
	Explain* explain = self->arg;
	explain->sent_objects++;
	explain->sent_size += buf_size(buf);
	buf_free(buf);
}

void
explain_init(Explain* self)
{
	self->active         = false;
	self->time_run_us    = 0;
	self->time_commit_us = 0;
	self->sent_objects   = 0;
	self->sent_size      = 0;
	portal_init(&self->portal, explain_portal, self);
}

void
explain_reset(Explain* self)
{
	self->active         = false;
	self->time_run_us    = 0;
	self->time_commit_us = 0;
	self->sent_objects   = 0;
	self->sent_size      = 0;
}

Buf*
explain(Explain*  self,
        Code*     coordinator,
        CodeData* data,
        Dispatch* dispatch,
        bool      profile)
{
	auto buf = buf_create(0);
	char name[16];
	int  name_size;

	// coordinator code
	Str str;
	str_init(&str);
	str_set_cstr(&str, "coordinator");
	op_dump(coordinator, data, buf, &str);

	// per shard code
	for (int order = 0; order < dispatch->set_size; order++)
	{
		auto req = dispatch_at(dispatch, order);
		if (! req)
			continue;
		name_size = snprintf(name, sizeof(name), "shard %d", order);
		str_set(&str, name, name_size);
		op_dump(&req->code, data, buf, &str);
	}

	// profiler stats
	if (profile)
	{
		uint64_t time_us = self->time_run_us + self->time_commit_us;
		buf_printf(buf, "%s", "\n");
		buf_printf(buf, "%s", "profiler\n");
		buf_printf(buf, "%s", "--------\n");
		buf_printf(buf, " time run:     %.3f ms\n", self->time_run_us / 1000.0);
		buf_printf(buf, " time commit:  %.3f ms\n", self->time_commit_us / 1000.0);
		buf_printf(buf, " time:         %.3f ms\n", time_us / 1000.0);
		buf_printf(buf, " sent objects: %d\n", self->sent_objects);
		buf_printf(buf, " sent total:   %d\n", self->sent_size);
	}

	str_set(&str, (char*)buf->start, buf_size(buf));
	auto string = make_string(&str);
	buf_free(buf);
	return string;
}
