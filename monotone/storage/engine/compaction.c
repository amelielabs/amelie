
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
compaction_apply(Engine* self, CompactReq* req)
{
	auto origin = req->part;

	// detach original partition
	service_remove(&self->service, origin);
	list_unlink(&origin->link);
	self->list_count--;

	// left
	if (req->l)
	{
		auto l = req->l;
		list_init(&l->link);
		list_append(&self->list, &l->link);
		self->list_count++;
		part_map_set(&self->map, l);

		if (heap_is_full(&l->heap))
			service_add(&self->service, l);
	}

	// right
	if (req->r)
	{
		auto r = req->r;
		list_init(&r->link);
		list_append(&self->list, &r->link);
		self->list_count++;
		part_map_set(&self->map, r);

		if (heap_is_full(&r->heap))
			service_add(&self->service, r);
	}
}

void
compaction(Engine*     self,
           CompactMgr* mgr,
           CompactReq* req,
           Part*       part)
{
	coroutine_cancel_pause(mn_self());

	Exception e;
	if (try(&e))
	{
		// prepare compact request
		req->op          = COMPACT_SPLIT_1;
		req->compression = self->compression;
		req->crc         = self->crc;
		req->part        = part;
		req->l           = NULL;
		req->r           = NULL;
		req->commit      = &part->heap.commit;

		// prepare heap for compaction
		heap_commit_set_2(req->commit);

		// 1st stage
		compact_mgr_run(mgr, req);

		// get an exclusive lock on the partition map range to
		// ensure no active transactions for this partition
		auto ref  = part_map_of(&self->map, part);
		auto lock = lock_lock(&ref->lock, false);

		// 2nd stage
		req->op = COMPACT_SPLIT_2;
		compact_mgr_run(mgr, req);

		// apply changes
		compaction_apply(self, req);

		// cleanup/free partition
		part_free(part);

		// unlock partition range
		lock_unlock(lock);
	}
	
	if (catch(&e))
	{}

	coroutine_cancel_resume(mn_self());
}
