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

struct LogIf
{
	void (*commit)(Log*, LogOp*);
	void (*abort)(Log*, LogOp*);
};

struct LogOp
{
	LogIf* iface;
	void*  iface_arg;
	Cmd    cmd;
	union {
		struct {
			Row*      row;
			Row*      row_prev;
			Snapshot* snapshot;
		};
		struct {
			Rel*      rel;
			int       rel_data;
		};
	};
};

struct Log
{
	Buf      op;
	Buf      data;
	int      count;
	WriteLog write_log;
	WriteCdc write_cdc;
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
	write_log_init(&self->write_log);
	write_cdc_init(&self->write_cdc);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	write_log_free(&self->write_log);
	write_cdc_free(&self->write_cdc);
}

static inline void
log_reset(Log* self)
{
	self->count = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	write_log_reset(&self->write_log);
	write_cdc_reset(&self->write_cdc);
}

static inline LogOp*
log_last(Log* self)
{
	return log_of(self, self->count - 1);
}

hot static inline LogOp*
log_dml(Log*      self,
        Cmd       cmd,
        LogIf*    iface,
        void*     iface_arg,
        Row*      row,
        Row*      row_prev,
        Snapshot* snapshot)
{
	auto op = (LogOp*)buf_emplace(&self->op, sizeof(LogOp));
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->cmd       = cmd;
	op->row       = row;
	op->row_prev  = row_prev;
	op->snapshot  = snapshot;
	self->count++;
	return op;
}

static inline void
log_cmd(Log*   self,
        Cmd    cmd,
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
	log_cmd(self, CMD_DDL, iface, iface_arg, rel);
}

hot static inline void
log_cdc(Log* self, Uuid* id, Columns* columns, Timezone* tz)
{
	// [cmd, id, data, data_size]
	auto last = log_last(self);
	auto row  = last->row;
	write_cdc_add_row(&self->write_cdc, last->cmd, id, row, columns, tz);
}

hot static inline void
log_persist_dml(Log* self, Uuid* id)
{
	// [cmd, id, row]
	auto last = log_last(self);
	auto row  = last->row;
	write_log_add(&self->write_log, last->cmd, id, (uint8_t*)row, row_size(row));
}

hot static inline void
log_persist_cmd(Log* self, Uuid* id, uint8_t* data)
{
	// [cmd, id, data]
	auto last = log_last(self);
	write_log_add_cmd(&self->write_log, last->cmd, id, data, json_sizeof(data));
}
