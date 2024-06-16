#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Indexate Indexate;

struct Indexate
{
	Table*       table;
	IndexConfig* index;
	Cluster*     cluster;
	Channel      channel;
};

static inline void
indexate_init(Indexate*    self,
              Cluster*     cluster,
              Table*       table,
              IndexConfig* index)
{
	self->table   = table;
	self->index   = index;
	self->cluster = cluster;
	channel_init(&self->channel);
}

static inline void
indexate_free(Indexate* self)
{
	channel_detach(&self->channel);
	channel_free(&self->channel);
}

static inline void
indexate_run(Indexate* self)
{
	channel_attach(&self->channel);

	// ask each node to indexate related partition
	list_foreach(&self->cluster->list)
	{
		auto node = list_at(Node, link);
		auto buf = msg_begin(RPC_INDEXATE);
		buf_write(buf, &self, sizeof(Indexate**));
		msg_end(buf);
		channel_write(&node->task.channel, buf);
	}

	// wait for completion
	int complete = 0;
	int errors   = 0;
	while (complete < self->cluster->list_count)
	{
		auto buf = channel_read(&self->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);
		complete++;
		if (msg->id == MSG_ERROR)
			errors++;
	}
	if (errors > 0)
		error("failed to create index");
}

static inline void
indexate_execute(Indexate* self, Uuid* node)
{
	// create new index content
	Exception e;
	if (enter(&e))
	{
		log("begin create index");
		part_list_indexate(&self->table->part_list, self->index, node);
	}
	Buf* buf;
	if (leave(&e)) {
		buf = msg_error(&so_self()->error);
	} else {
		buf = msg_begin(MSG_OK);
		msg_end(buf);
	}

	log("complete");
	channel_write(&self->channel, buf);
}
