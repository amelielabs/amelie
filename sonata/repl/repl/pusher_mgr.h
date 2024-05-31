#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct PusherMgr PusherMgr;

struct PusherMgr
{
	List list;
	int  list_count;
	Wal* wal;
};

static inline void
pusher_mgr_init(PusherMgr* self, Wal* wal)
{
	self->wal = wal;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
pusher_mgr_start(PusherMgr* self, NodeMgr* node_mgr)
{
	list_foreach_safe(&node_mgr->list)
	{
		auto node = list_at(Node, link);
		auto pusher = pusher_allocate(self->wal, node);
		list_append(&self->list, &pusher->link);
		self->list_count++;
		pusher_start(pusher);
	}
}

static inline void
pusher_mgr_stop(PusherMgr* self)
{
	list_foreach(&self->list)
	{
		auto pusher = list_at(Pusher, link);
		pusher_stop(pusher);
		pusher_free(pusher);
	}
	list_init(&self->list);
	self->list_count = 0;
}
