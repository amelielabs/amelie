
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>

static void
service_main(void* arg)
{
	Service* self = arg;
	coroutine_set_name(mn_self(), "service");

	event_signal(&self->event);
	for (;;)
	{
		event_wait(&self->event, -1);
		for (;;)
		{
			auto part = service_next(self);
			if (part == NULL)
				break;

			self->active = true;

			compaction(self->engine, self->compact_mgr,
			          &self->compact_req,
			           part);

			self->active = false;
		}
	}
}

void
service_init(Service* self, CompactMgr* mgr, void* arg)
{
	self->active      = false;
	self->list_count  = 0;
	self->worker_id   = UINT64_MAX;
	self->compact_mgr = mgr;
	self->engine      = arg;
	event_init(&self->event);
	compact_req_init(&self->compact_req);
	list_init(&self->list);
}

void
service_free(Service* self)
{
	unused(self);
}

void
service_start(Service* self)
{
	assert(self->worker_id == UINT64_MAX);
	self->worker_id = coroutine_create(service_main, self);
}

void
service_stop(Service* self)
{
	if (self->worker_id != UINT64_MAX)
	{
		coroutine_kill(self->worker_id);
		self->worker_id = UINT64_MAX;
	}
}

void
service_add(Service* self, Part* part)
{
	if (! list_empty(&part->link_service))
		return;
	list_append(&self->list, &part->link_service);
	self->list_count++;
	event_signal(&self->event);
}

void
service_remove(Service* self, Part* part)
{
	if (list_empty(&part->link_service))
		return;
	list_unlink(&part->link_service);
	self->list_count--;
	assert(self->list_count >= 0);
	list_init(&part->link_service);
}

Part*
service_next(Service* self)
{
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link_service);
		if (part->service)
			continue;
		part->service = true;
		return part;
	}
	return NULL;
}
