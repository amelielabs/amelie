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

typedef struct DstLogOp DstLogOp;
typedef struct DstLog   DstLog;

enum
{
	DST_OP_PUBLISH,
	DST_OP_INSERT,
	DST_OP_UPSERT,
	DST_OP_UPDATE,
	DST_OP_DELETE
};

struct DstLogOp
{
	int     op;
	DstRel* rel;
	DstKey  prev;
	DstKey  key;
};

struct DstLog
{
	Buf ops;
	int ops_count;
	Buf sql;
};

static inline DstLogOp*
dst_log_at(DstLog* self, int at)
{
	return &((DstLogOp*)self->ops.start)[at];
}

static inline void
dst_log_init(DstLog* self)
{
	self->ops_count = 0;
	buf_init(&self->ops);
	buf_init(&self->sql);
}

static inline void
dst_log_free(DstLog* self)
{
	buf_free(&self->ops);
	buf_free(&self->sql);
}

static inline void
dst_log_reset(DstLog* self)
{
	self->ops_count = 0;
	buf_reset(&self->ops);
	buf_reset(&self->sql);
}

static inline bool
dst_log_empty(DstLog* self)
{
	return !self->ops_count;
}

static inline DstLogOp*
dst_log_add(DstLog* self)
{
	auto op = (DstLogOp*)buf_emplace(&self->ops, sizeof(DstLogOp));
	memset(op, 0, sizeof(DstLogOp));
	self->ops_count++;
	return  op;
}
