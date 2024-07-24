#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
msg_create_nothrow(BufCache* cache, int id, int reserve)
{
	auto buf = buf_create_nothrow(cache, sizeof(Msg) + reserve);
	if (unlikely(buf == NULL))
		return NULL;
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
