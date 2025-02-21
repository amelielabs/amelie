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

typedef struct LogIf     LogIf;
typedef struct LogOp     LogOp;
typedef struct LogRow    LogRow;
typedef struct LogHandle LogHandle;
typedef struct Log       Log;

struct LogIf
{
	void (*commit)(Log*, LogOp*);
	void (*abort)(Log*, LogOp*);
};

struct LogOp
{
	Cmd    cmd;
	LogIf* iface;
	void*  iface_arg;
	int    pos;
};

struct LogRow
{
	Row*  row;
	Row*  row_prev;
	Keys* keys;
};

struct LogHandle
{
	void* handle;
	void* arg;
	Buf*  data;
};

struct Log
{
	Buf      op;
	Buf      data;
	int      count;
	int      count_handle;
	LogWrite log_write;
	List     link;
};

always_inline static inline LogOp*
log_of(Log* self, int pos)
{
	return &((LogOp*)self->op.start)[pos];
}

always_inline static inline void*
log_data_of(Log* self, LogOp* op)
{
	return self->data.start + op->pos;
}

always_inline static inline LogRow*
log_row_of(Log* self, LogOp* op)
{
	return log_data_of(self, op);
}

always_inline static inline LogHandle*
log_handle_of(Log* self, LogOp* op)
{
	return log_data_of(self, op);
}

static inline void
log_init(Log* self)
{
	self->count        = 0;
	self->count_handle = 0;
	buf_init(&self->op);
	buf_init(&self->data);
	log_write_init(&self->log_write);
	list_init(&self->link);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	log_write_free(&self->log_write);
}

static inline void
log_reset(Log* self)
{
	self->count        = 0;
	self->count_handle = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	log_write_reset(&self->log_write);
	list_init(&self->link);
}

static inline void
log_truncate(Log* self)
{
	self->count--;
	auto op = log_of(self, self->count);
	buf_truncate(&self->op, sizeof(LogOp));
	buf_truncate(&self->data, buf_size(&self->data) - op->pos);
}

hot static inline LogRow*
log_row(Log*   self,
        Cmd    cmd,
        LogIf* iface,
        void*  iface_arg,
        Keys*  keys,
        Row*   row,
        Row*   row_prev)
{
	// op
	LogOp* op = buf_claim(&self->op, sizeof(LogOp));
	op->cmd       = cmd;
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->pos       = buf_size(&self->data);
	self->count++;

	// row data
	LogRow* ref = buf_claim(&self->data, sizeof(LogRow));
	ref->keys     = keys;
	ref->row      = row;
	ref->row_prev = row_prev;
	return ref;
}

hot static inline void
log_persist(Log* self, uint64_t partition)
{
	auto op = log_of(self, self->count - 1);
	// [cmd, partition, row]
	auto ref = log_row_of(self, op);
	log_write_add(&self->log_write, op->cmd, partition, ref->row);
}

static inline void
log_handle(Log*   self,
           Cmd    cmd,
           LogIf* iface,
           void*  iface_arg,
           void*  handle,
           void*  arg,
           Buf*   data)
{
	// op
	LogOp* op = buf_claim(&self->op, sizeof(LogOp));
	op->cmd       = cmd;
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->pos       = buf_size(&self->data);
	self->count++;
	self->count_handle++;

	// handle data
	LogHandle* ref = buf_claim(&self->data, sizeof(LogHandle));
	ref->handle = handle;
	ref->arg    = arg;
	ref->data   = data;

	// [cmd, data]
	log_write_add_op(&self->log_write, cmd, data);
}
