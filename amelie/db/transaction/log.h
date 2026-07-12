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
	LOG_ACK,
	LOG_DDL
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
	LogCdc cdc;
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
	log_cdc_init(&self->cdc);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	log_cdc_free(&self->cdc);
}

static inline void
log_reset(Log* self)
{
	self->count = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	log_cdc_reset(&self->cdc);
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
        Row*      row_prev,
        Timeline* timeline)
{
	auto op = (LogOp*)buf_emplace(&self->op, sizeof(LogOp));
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->cmd       = cmd;
	op->row       = row;
	op->row_prev  = row_prev;
	op->timeline  = timeline;
	self->count++;
	return op;
}

static inline void
log_cmd(Log*   self,
        int    cmd,
        LogIf* iface,
        void*  iface_arg,
        Rel*   rel)
{
	auto op = (LogOp*)buf_emplace(&self->op, sizeof(LogOp));
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->cmd       = cmd;
	op->rel       = rel;
	op->rel_data  = buf_size(&self->data);
	self->count++;
}

static inline void
log_ddl(Log*   self,
        LogIf* iface,
        void*  iface_arg,
        Rel*   rel)
{
	log_cmd(self, LOG_DDL, iface, iface_arg, rel);
}

hot static inline void
log_cdc(Log* self, int cmd, Uuid* id, Row* row, Columns* columns, Timezone* tz)
{
	// [cmd, id, data, data_size]
	log_cdc_add_row(&self->cdc, cmd, id, row, columns, tz);
}
