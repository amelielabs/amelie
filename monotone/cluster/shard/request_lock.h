#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RequestLock RequestLock;

struct RequestLock
{
	Condition* on_lock;
	Condition* on_unlock;
};

always_inline hot static inline RequestLock*
request_lock_of(Buf* buf)
{
	return *(RequestLock**)msg_of(buf)->data;
}

static inline void
request_lock_init(RequestLock* self)
{
	self->on_lock   = NULL;
	self->on_unlock = NULL;
}

static inline Buf*
request_lock_msg(RequestLock* self)
{
	auto buf = msg_create(RPC_CAT_LOCK_REQ);
	buf_write(buf, &self, sizeof(void**));
	msg_end(buf);
	return buf;
}
