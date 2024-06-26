#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Build Build;

typedef enum
{
	BUILD_NONE,
	BUILD_RECOVER,
	BUILD_COLUMN_ADD,
	BUILD_COLUMN_DROP,
	BUILD_INDEX
} BuildType;

struct Build
{
	BuildType    type;
	Table*       table;
	IndexConfig* index;
	Cluster*     cluster;
	Channel      channel;
};

static inline void
build_init(Build*       self,
           BuildType    type,
           Cluster*     cluster,
           Table*       table,
           IndexConfig* index)
{
	self->type    = type;
	self->table   = table;
	self->index   = index;
	self->cluster = cluster;
	channel_init(&self->channel);
}

static inline void
build_free(Build* self)
{
	channel_detach(&self->channel);
	channel_free(&self->channel);
}

static inline void
build_run_first(Build* self)
{
	// ask first node to build related partition
	auto route = router_first(&self->cluster->router);
	auto buf = msg_begin(RPC_BUILD);
	buf_write(buf, &self, sizeof(Build**));
	msg_end(buf);
	channel_write(route->channel, buf);

	// wait for completion
	buf = channel_read(&self->channel, -1);
	auto msg = msg_of(buf);
	guard(buf_free, buf);

	if (msg->id == MSG_ERROR)
		msg_error_throw(buf);
}

static inline void
build_run_all(Build* self)
{
	// ask each node to build related partition
	list_foreach(&self->cluster->list)
	{
		auto node = list_at(Node, link);
		auto buf = msg_begin(RPC_BUILD);
		buf_write(buf, &self, sizeof(Build**));
		msg_end(buf);
		channel_write(&node->task.channel, buf);
	}

	// wait for completion
	Buf* error = NULL;	
	int  complete;
	for (complete = 0; complete < self->cluster->list_count;
	     complete++)
	{
		auto buf = channel_read(&self->channel, -1);
		auto msg = msg_of(buf);
		if (msg->id == MSG_ERROR && !error)
		{
			error = buf;
			continue;
		}
		buf_free(buf);
	}
	if (error)
	{
		guard_buf(error);
		msg_error_throw(error);
	}
}

static inline void
build_run(Build* self)
{
	channel_attach(&self->channel);

	// run on first node (for reference table) or use whole cluster
	if (self->table && self->table->config->reference)
		build_run_first(self);
	else
		build_run_all(self);
}

static inline void
build_execute(Build* self, Uuid* node)
{
	Exception e;
	if (enter(&e))
	{
		switch (self->type) {
		case BUILD_RECOVER:
			// restore last checkpoint partitions related to the node
			recover_checkpoint(self->cluster->db, node);
			break;
		case BUILD_INDEX:
		{
			auto part = part_list_match(&self->table->part_list, node);
			if (! part)
				break;
			// build new index content for current node
			PartBuild pb;
			part_build_init(&pb, PART_BUILD_INDEX, part, NULL, 0, NULL, self->index);
			part_build(&pb);
			break;
		}
		case BUILD_COLUMN_ADD:
		{
			break;
		}
		case BUILD_COLUMN_DROP:
		{
			break;
		}
		case BUILD_NONE:
			break;
		}
	}
	Buf* buf;
	if (leave(&e)) {
		buf = msg_error(&so_self()->error);
	} else {
		buf = msg_begin(MSG_OK);
		msg_end(buf);
	}
	channel_write(&self->channel, buf);
}

extern RecoverIf build_if;
