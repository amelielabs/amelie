#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct LogOp LogOp;
typedef struct Log   Log;

typedef enum
{
	LOG_WRITE,
	LOG_WRITE_HANDLE
} LogType;

typedef enum
{
	// row
	LOG_REPLACE,
	LOG_DELETE,
	// handle
	LOG_CREATE_TABLE,
	LOG_DROP_TABLE,
	LOG_ALTER_TABLE,
	LOG_CREATE_META,
	LOG_DROP_META
} LogCmd;

struct LogOp
{
	LogType type;
	LogCmd  cmd;
	union
	{
		struct
		{
			void*   heap;
			Locker* locker;
			Row*    row;
			Row*    prev;
		} write;
		struct
		{
			void* handle_mgr;
			void* handle;
			void* handle_prev;
			Buf*  data;
		} write_handle;
	};
};

struct Log
{
	Buf      op; 
	int      count;
	uint64_t lsn;
};

static inline LogOp*
log_of(Log* self, int pos)
{
	auto op = (LogOp*)self->op.start;
	return &op[pos];
}

static inline void
log_init(Log* self)
{
	self->count = 0;
	self->lsn   = 0;
	buf_init(&self->op);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
}

static inline void
log_reset(Log* self)
{
	self->count = 0;
	self->lsn   = 0;
	buf_reset(&self->op);
}

static inline void
log_reserve(Log* self)
{
	buf_reserve(&self->op, sizeof(LogOp));
}

static inline void
log_add_write(Log*    self,
              LogCmd  cmd,
              void*   heap,
              Locker* locker,
              Row*    row,
              Row*    prev)
{
	buf_reserve(&self->op, sizeof(LogOp));
	auto op = (LogOp*)self->op.position;
	op->type         = LOG_WRITE;
	op->cmd          = cmd;
	op->write.heap   = heap;
	op->write.locker = locker;
	op->write.row    = row;
	op->write.prev   = prev;
	buf_advance(&self->op, sizeof(LogOp));
	self->count++;
}

static inline void
log_add_write_handle(Log*   self,
                     LogCmd cmd,
                     void*  handle_mgr,
                     void*  handle,
                     void*  handle_prev,
                     Buf*   data)
{
	buf_reserve(&self->op, sizeof(LogOp));
	auto op = (LogOp*)self->op.position;
	op->type                     = LOG_WRITE_HANDLE;
	op->cmd                      = cmd;
	op->write_handle.handle_mgr  = handle_mgr;
	op->write_handle.handle      = handle;
	op->write_handle.handle_prev = handle_prev;
	op->write_handle.data        = data;
	buf_advance(&self->op, sizeof(LogOp));
	self->count++;
}

static inline void
log_delete_last(Log* self)
{
	assert(self->count > 0);
	self->count--;
	buf_truncate(&self->op, sizeof(LogOp));
}
