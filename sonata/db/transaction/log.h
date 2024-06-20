#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
	// dml (row)
	LOG_REPLACE,
	LOG_DELETE,
	// ddl (handle)
	LOG_SCHEMA_CREATE,
	LOG_SCHEMA_DROP,
	LOG_SCHEMA_RENAME,
	LOG_TABLE_CREATE,
	LOG_TABLE_DROP,
	LOG_TABLE_RENAME,
	LOG_INDEX_CREATE,
	LOG_INDEX_DROP,
	LOG_INDEX_RENAME,
	LOG_VIEW_CREATE,
	LOG_VIEW_DROP,
	LOG_VIEW_RENAME
} LogCmd;

typedef void (*LogCommit)(LogOp*);
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
	int      count;
	int      count_handle;
	uint64_t lsn;
	LogSet   log_set;
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
	self->count        = 0;
	self->count_handle = 0;
	self->lsn          = 0;
	buf_init(&self->op);
	log_set_init(&self->log_set);
	list_init(&self->link);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	log_set_free(&self->log_set);
}

static inline void
log_reset(Log* self)
{
	self->count        = 0;
	self->count_handle = 0;
	self->lsn          = 0;
	buf_reset(&self->op);
	log_set_reset(&self->log_set);
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
        uint64_t  partition,
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

	// [cmd, partition, row]
	log_set_add(&self->log_set, cmd, partition, row);
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

	// [cmd, data]
	log_set_add_op(&self->log_set, cmd, data);
}
