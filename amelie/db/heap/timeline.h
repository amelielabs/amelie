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

typedef struct Timeline Timeline;

struct Timeline
{
	int64_t timeline;
	Rel*    rel;
	List    link;
};

static inline void
timeline_init(Timeline* self)
{
	self->timeline = 0;
	self->rel      = NULL;
	list_init(&self->link);
}

static inline void
timeline_set_timeline(Timeline* self, uint64_t value)
{
	self->timeline = value;
}

static inline void
timeline_copy(Timeline* self, Timeline* from)
{
	timeline_set_timeline(self, from->timeline);
}
