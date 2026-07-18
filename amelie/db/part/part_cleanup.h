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

typedef struct PartCleanup PartCleanup;

struct PartCleanup
{
	Msg      msg;
	Part*    part;
	uint32_t timeline;
	Buf*     buf;
};

static inline PartCleanup*
part_cleanup_of(Msg* msg)
{
	return (PartCleanup*)msg;
}

static inline Msg*
part_cleanup_allocate(Part* part, uint32_t timeline)
{
	auto buf = buf_create();
	auto self = (PartCleanup*)buf_emplace(buf, sizeof(PartCleanup));
	msg_init(&self->msg, MSG_CLEANUP);
	self->part     = part;
	self->timeline = timeline;
	self->buf      = buf;
	return &self->msg;
}

static inline void
part_cleanup_free(PartCleanup* self)
{
	buf_free(self->buf);
}

void part_cleanup_run(PartCleanup*);
