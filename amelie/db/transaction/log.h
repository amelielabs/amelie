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

typedef struct LogIf LogIf;
typedef struct LogOp LogOp;
typedef struct Log   Log;
typedef struct Part  Part;

struct LogIf
{
	void (*commit)(Log*, LogOp*);
	void (*abort)(Log*, LogOp*);
};

struct LogOp
{
	LogIf* iface;
	void*  iface_arg;
	union {
		struct {
			Part* part;
			Row*  row;
			Row*  row_prev;
		};
		struct {
			Rel*  rel;
			int   rel_data;
		};
	};
};

struct Log
{
	Buf op;
	Buf data;
	int count;
};

always_inline static inline LogOp*
log_of(Log* self, int pos)
{
	return &((LogOp*)self->op.start)[pos];
}

always_inline static inline void*
log_data_of(Log* self, LogOp* op)
{
	return self->data.start + op->rel_data;
}

static inline void
log_init(Log* self)
{
	self->count = 0;
	buf_init(&self->op);
	buf_init(&self->data);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
}

static inline void
log_reset(Log* self)
{
	self->count = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
}

static inline LogOp*
log_last(Log* self)
{
	return log_of(self, self->count - 1);
}

hot static inline LogOp*
log_dml(Log*   self,
        LogIf* iface,
        void*  iface_arg,
        Part*  part,
        Row*   row)
{
	auto op = (LogOp*)buf_emplace(&self->op, sizeof(LogOp));
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->part      = part;
	op->row       = row;
	op->row_prev  = NULL;
	self->count++;
	return op;
}

static inline void
log_ddl(Log*   self,
        LogIf* iface,
        void*  iface_arg,
        Rel*   rel)
{
	auto op = (LogOp*)buf_emplace(&self->op, sizeof(LogOp));
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->rel       = rel;
	op->rel_data  = buf_size(&self->data);
	self->count++;
}
