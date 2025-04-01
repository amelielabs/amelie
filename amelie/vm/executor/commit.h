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

typedef struct Commit Commit;

struct Commit
{
	uint64_t  list_max;
	int       list_count;
	List      list;
	Backlog*  backlogs;
	WriteList write;
};

static inline void
commit_init(Commit* self)
{
	self->list_max   = 0;
	self->list_count = 0;
	self->backlogs   = NULL;
	list_init(&self->list);
	write_list_init(&self->write);
}

static inline void
commit_reset(Commit* self)
{
	auto backlog = self->backlogs;
	while (backlog)
	{
		auto next = backlog->commit_next;
		backlog->commit = false;
		backlog->commit_next = NULL;
		backlog = next;
	}
	self->backlogs = NULL;
	self->list_max   = 0;
	self->list_count = 0;
	list_init(&self->list);
	write_list_reset(&self->write);
}

hot static inline void
commit_add(Commit* self, Dtr* dtr)
{
	// add transaction to the list
	list_append(&self->list, &dtr->link_commit);
	self->list_count++;

	// track max transaction id
	if (dtr->id > self->list_max)
		self->list_max = dtr->id;

	// create a unique list of all partitions
	auto dispatch = &dtr->dispatch;
	for (auto i = 0; i < dispatch->steps; i++)
	{
		auto step = dispatch_at(dispatch, i);
		list_foreach(&step->list.list)
		{
			auto req = list_at(Req, link);
			auto backlog = req->arg.backlog;
			if (backlog->commit)
				continue;
			backlog->commit = true;
			backlog->commit_next = self->backlogs;
			self->backlogs = backlog;
		}
	}
}
