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
	List      list_parts;
	WriteList write;
};

static inline void
commit_init(Commit* self)
{
	self->list_max   = 0;
	self->list_count = 0;
	list_init(&self->list);
	list_init(&self->list_parts);
	write_list_init(&self->write);
}

static inline void
commit_reset(Commit* self)
{
	list_foreach_safe(&self->list_parts)
	{
		auto part = list_at(Part, link_commit);
		list_init(&part->link_commit);
	}
	self->list_max   = 0;
	self->list_count = 0;
	list_init(&self->list);
	list_init(&self->list_parts);
	write_list_reset(&self->write);
}

hot static inline void
commit_add(Commit* self, Dtr* dtr)
{
	list_append(&self->list, &dtr->link_commit);
	self->list_count++;

	// track max transaction id
	if (dtr->id > self->list_max)
		self->list_max = dtr->id;

	// create a unique list of all queues (partitions)
	auto dispatch = &dtr->dispatch;
	for (auto i = 0; i < dispatch->steps; i++)
	{
		auto step = dispatch_at(dispatch, i);
		list_foreach(&step->list.list)
		{
			auto job = list_at(Job, link);
			if (! job->arg.part)
				continue;
			if (list_empty(&job->arg.part->link_commit))
				list_append(&self->list_parts, &job->arg.part->link_commit);
		}
	}
}
