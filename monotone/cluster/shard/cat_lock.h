#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct CatLock CatLock;

struct CatLock
{
	Condition* on_lock;
	Condition* on_unlock;
};

always_inline hot static inline CatLock*
cat_lock_of(Buf* buf)
{
	return *(CatLock**)msg_of(buf)->data;
}

static inline void
cat_lock_init(CatLock* self)
{
	self->on_lock   = NULL;
	self->on_unlock = NULL;
}

static inline Buf*
cat_lock_msg(CatLock* self)
{
	auto buf = msg_create(RPC_CAT_LOCK_REQ);
	buf_write(buf, &self, sizeof(void**));
	msg_end(buf);
	return buf;
}
