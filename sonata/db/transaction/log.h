#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct LogOp LogOp;
typedef struct Log   Log;

typedef enum
{
	LOG_WRITE,
	LOG_CREATE,
	LOG_DROP,
	LOG_ALTER
} LogType;

typedef enum
{
	// row
	LOG_REPLACE,
	LOG_DELETE,
	// handle
	LOG_SCHEMA_CREATE,
	LOG_SCHEMA_DROP,
	LOG_SCHEMA_RENAME,
	LOG_TABLE_CREATE,
	LOG_TABLE_DROP,
	LOG_TABLE_RENAME,
	LOG_VIEW_CREATE,
	LOG_VIEW_DROP,
	LOG_VIEW_RENAME
} LogCmd;

typedef void (*LogCommit)(LogOp*, uint64_t);
typedef void (*LogAbort)(LogOp*);

struct LogOp
{
	LogType   type;
	LogCmd    cmd;
	LogCommit commit;
	LogAbort  abort;
	void*     arg;
	union
	{
		struct
		{
			Row* row;
			Row* prev;
		} row;
		struct
		{
			void* handle;
			Buf*  data;
		} handle;
	};
};

struct Log
{
	Buf      op; 
	Buf      data;
	Iov      data_iov;
	int      count;
	int      count_persistent;
	int      count_handle;
	uint64_t lsn;
	List     link;
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
	self->count            = 0;
	self->count_persistent = 0;
	self->count_handle     = 0;
	self->lsn              = 0;
	buf_init(&self->op);
	buf_init(&self->data);
	iov_init(&self->data_iov);
	list_init(&self->link);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	iov_free(&self->data_iov);
}

static inline void
log_reset(Log* self)
{
	self->count            = 0;
	self->count_persistent = 0;
	self->count_handle     = 0;
	self->lsn              = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	iov_reset(&self->data_iov);
	list_init(&self->link);
}

static inline void
log_reserve(Log* self)
{
	buf_reserve(&self->op, sizeof(LogOp));
}

hot static inline void
log_row(Log*      self,
        LogCmd    cmd,
        LogCommit commit,
        LogAbort  abort,
        void*     arg,
        bool      persistent,
        uint64_t  storage,
        Def*      def,
        Row*      row,
        Row*      prev)
{
	buf_reserve(&self->op, sizeof(LogOp));

	auto op = (LogOp*)self->op.position;
	op->type     = LOG_WRITE;
	op->cmd      = cmd;
	op->commit   = commit;
	op->abort    = abort;
	op->arg      = arg;
	op->row.row  = row;
	op->row.prev = prev;
	buf_advance(&self->op, sizeof(LogOp));
	self->count++;
	if (! persistent)
		return;

	// todo: [cmd, storage, row]
	(void)storage;
	iov_add(&self->data_iov, row_data(row, def), row_data_size(row, def));

	self->count_persistent++;
}

static inline void
log_handle(Log*      self,
           LogType   type,
           LogCmd    cmd,
           LogCommit commit,
           LogAbort  abort,
           void*     arg,
           void*     handle,
           Buf*      data)
{
	buf_reserve(&self->op, sizeof(LogOp));
	auto op = (LogOp*)self->op.position;
	op->type          = type;
	op->cmd           = cmd;
	op->commit        = commit;
	op->abort         = abort;
	op->arg           = arg;
	op->handle.handle = handle;
	op->handle.data   = data;
	buf_advance(&self->op, sizeof(LogOp));
	self->count++;
	self->count_handle++;

	// todo: [cmd, data]
	self->count_persistent++;
}
