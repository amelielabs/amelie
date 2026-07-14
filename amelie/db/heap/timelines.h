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
	List     list;
	int      list_count;
	Timeline main;
};

static inline void
timelines_init(Timelines* self, Rel* rel, uint32_t state)
{
	// starts from 1
	auto main = &self->main;
	timeline_init(main);
	main->main     = true;
	main->rel      = rel;
	main->timeline = state;

	self->list_count = 0;
	list_init(&self->list);
}

static inline void
timelines_add(Timelines* self, Timeline* timeline)
{
	list_append(&self->list, &timeline->link);
	self->list_count++;

	// update main timeline max (max of children timeline)
	auto main = &self->main;
	if (timeline->timeline > main->timeline_max)
		main->timeline_max = timeline->timeline;
}

static inline void
timelines_remove(Timelines* self, Timeline* timeline)
{
	list_unlink(&timeline->link);
	self->list_count--;

	// update main timeline max
	auto main = &self->main;

	// (zero is not used by timelines)
	int64_t timeline_max = 0;
	list_foreach(&self->list)
	{
		auto timeline = list_at(Timeline, link);
		if (timeline->timeline > timeline_max)
			timeline_max = timeline->timeline;
	}
	main->timeline_max = timeline_max;
}
