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
	bool    main;
	int64_t timeline;
	int64_t timeline_max;
	Rel*    rel;
	List    link;
};

static inline void
timeline_init(Timeline* self)
{
	self->main         = false;
	self->timeline     = 0;
	self->timeline_max = 0;
	self->rel          = NULL;
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

static inline void
timeline_read(Timeline* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_INT, "timeline", &self->timeline },
		{ 0,           NULL,       NULL           },
	};
	decode_obj(obj, "timeline", pos);
}

static inline void
timeline_write(Timeline* self, Buf* buf, int flags)
{
	encode_obj(buf);

	// timeline
	encode_raw(buf, "timeline", 8);
	encode_int(buf, self->timeline);

	// timeline_max
	if (flags_has(flags, FMETRICS))
	{
		encode_raw(buf, "timeline_max", 12);
		encode_int(buf, self->timeline_max);
	}

	encode_obj_end(buf);
}
