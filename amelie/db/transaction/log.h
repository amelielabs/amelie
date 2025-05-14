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

typedef struct LogIf       LogIf;
typedef struct LogOp       LogOp;
typedef struct LogRow      LogRow;
typedef struct LogRelation LogRelation;
typedef struct Log         Log;

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
	Row* row;
	Row* row_prev;
};

struct LogRelation
{
	void* relation;
	Buf*  data;
};

struct Log
{
	Buf      op;
	Buf      data;
	int      count;
	int      count_relation;
	WriteLog write_log;
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

always_inline static inline LogRelation*
log_relation_of(Log* self, LogOp* op)
{
	return log_data_of(self, op);
}

static inline void
log_init(Log* self)
{
	self->count          = 0;
	self->count_relation = 0;
	buf_init(&self->op);
	buf_init(&self->data);
	write_log_init(&self->write_log);
	list_init(&self->link);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	write_log_free(&self->write_log);
}

static inline void
log_reset(Log* self)
{
	self->count          = 0;
	self->count_relation = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	write_log_reset(&self->write_log);
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
	write_log_add(&self->write_log, op->cmd, partition, ref->row);
}

static inline void
log_relation(Log*   self,
             Cmd    cmd,
             LogIf* iface,
             void*  iface_arg,
             void*  relation,
             Buf*   data)
{
	// op
	LogOp* op = buf_claim(&self->op, sizeof(LogOp));
	op->cmd       = cmd;
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->pos       = buf_size(&self->data);
	self->count++;
	self->count_relation++;

	// relation data
	LogRelation* rel = buf_claim(&self->data, sizeof(LogRelation));
	rel->relation = relation;
	rel->data     = data;

	// [cmd, data]
	write_log_add_op(&self->write_log, cmd, data);
}
