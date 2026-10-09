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

typedef struct Timelines Timelines;

struct Timelines
{
	Timeline main;
	List     list;
	int      list_count;
	uint32_t max;
};

static inline void
timelines_init(Timelines* self, Rel* rel)
{
	auto main = &self->main;
	timeline_init(main);
	main->rel        = rel;
	main->timeline   = 0;
	self->max        = 0;
	self->list_count = 0;
	list_init(&self->list);
}

static inline void
timelines_add(Timelines* self, Timeline* timeline)
{
	list_append(&self->list, &timeline->link);
	self->list_count++;
	if (timeline->timeline > self->max)
		self->max = timeline->timeline;
}

static inline void
timelines_remove(Timelines* self, Timeline* timeline)
{
	list_unlink(&timeline->link);
	self->list_count--;
}
