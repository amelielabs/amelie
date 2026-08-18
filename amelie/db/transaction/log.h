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

enum
{
	LOG_REPLACE,
	LOG_DELETE,
	LOG_PUBLISH,
	LOG_DDL,
	LOG_REQUEST
};

struct LogIf
{
	void (*commit)(Log*, LogOp*);
	void (*abort)(Log*, LogOp*);
};

struct LogOp
{
	LogIf* iface;
	void*  iface_arg;
	int    cmd;
	union {
		struct {
			Row*      row;
			Row*      row_prev;
			Timeline* timeline;
		};
		struct {
			Rel*      rel;
			int       rel_data;
		};
	};
};

struct Log
{
	Buf    op;
	Buf    data;
	int    count;
	CdcLog cdc;
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
	cdc_log_init(&self->cdc);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	cdc_log_free(&self->cdc);
}

static inline void
log_reset(Log* self)
{
	self->count = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	cdc_log_reset(&self->cdc);
}

static inline LogOp*
log_last(Log* self)
{
	return log_of(self, self->count - 1);
}

hot static inline LogOp*
log_dml(Log*      self,
        int       cmd,
        LogIf*    iface,
        void*     iface_arg,
        Row*      row,
        Timeline* timeline)
{
	auto op = (LogOp*)buf_emplace(&self->op, sizeof(LogOp));
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->cmd       = cmd;
	op->row       = row;
	op->row_prev  = NULL;
	op->timeline  = timeline;
	self->count++;
	return op;
}

hot static inline LogOp*
log_replace(Log*      self,
            LogIf*    iface,
            void*     iface_arg,
            Row*      row,
            Timeline* timeline)
{
	return log_dml(self, LOG_REPLACE, iface, iface_arg, row, timeline);
}

hot static inline LogOp*
log_delete(Log*      self,
           LogIf*    iface,
           void*     iface_arg,
           Row*      row,
           Timeline* timeline)
{
	return log_dml(self, LOG_DELETE, iface, iface_arg, row, timeline);
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
	op->cmd       = LOG_DDL;
	op->rel       = rel;
	op->rel_data  = buf_size(&self->data);
	self->count++;
}
