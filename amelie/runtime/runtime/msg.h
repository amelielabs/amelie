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

typedef struct Msg Msg;

struct Msg
{
	uint32_t size:24;
	uint32_t id:8;
	uint8_t  data[];
} packed;

static inline Msg*
msg_of(Buf* buf)
{
	return (Msg*)buf->start;
}

static inline Buf*
msg_create_as(BufMgr* mgr, int id, int reserve)
{
	auto buf = buf_mgr_create(mgr, sizeof(Msg) + reserve);
	auto msg = msg_of(buf);
	msg->size = sizeof(Msg);
	msg->id   = id;
	buf_advance(buf, sizeof(Msg));
	return buf;
}

static inline int
msg_data_size(Msg* msg)
{
	return msg->size - sizeof(Msg);
}

static inline void
msg_end(Buf* self)
{
	msg_of(self)->size = buf_size(self);
}
