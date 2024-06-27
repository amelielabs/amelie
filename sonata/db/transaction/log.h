#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct LogIf     LogIf;
typedef struct LogOp     LogOp;
typedef struct LogRow    LogRow;
typedef struct LogHandle LogHandle;
typedef struct Log       Log;

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
	LOG_TABLE_COLUMN_RENAME,
	LOG_TABLE_COLUMN_ADD,
	LOG_TABLE_COLUMN_DROP,
	LOG_INDEX_CREATE,
	LOG_INDEX_DROP,
	LOG_INDEX_RENAME,
	LOG_VIEW_CREATE,
	LOG_VIEW_DROP,
	LOG_VIEW_RENAME
} LogCmd;

struct LogIf
{
	void (*commit)(Log*, LogOp*);
	void (*abort)(Log*, LogOp*);
};

struct LogOp
{
	LogCmd cmd;
	LogIf* iface;
	void*  iface_arg;
	int    pos;
};

struct LogRow
{
	Keys* keys;
	Ref   rows[];
};

struct LogHandle
{
	void* handle;
	Buf*  data;
};

struct Log
{
	Buf    op;
	Buf    data;
	int    count;
	int    count_handle;
	LogSet log_set;
	List   link;
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

always_inline static inline Ref*
log_row_of(Log* self, LogOp* op,  Ref** b)
{
	auto row = (LogRow*)log_data_of(self, op);
	auto a = &row->rows[0];
	if (b)
		*b = (Ref*)((uint8_t*)a + row->keys->key_size);
	return a;
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
	log_set_init(&self->log_set);
	list_init(&self->link);
}

static inline void
log_free(Log* self)
{
	buf_free(&self->op);
	buf_free(&self->data);
	log_set_free(&self->log_set);
}

static inline void
log_reset(Log* self)
{
	self->count        = 0;
	self->count_handle = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	log_set_reset(&self->log_set);
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

hot static inline void
log_row(Log*   self,
        LogCmd cmd,
        LogIf* iface,
        void*  iface_arg,
        Keys*  keys,
        Ref**  key,
        Ref**  prev)
{
	// op
	LogOp* op = buf_claim(&self->op, sizeof(LogOp));
	op->cmd       = cmd;
	op->iface     = iface;
	op->iface_arg = iface_arg;
	op->pos       = buf_size(&self->data);
	self->count++;

	// row data
	int size = 2 * keys->key_size;
	LogRow* row = buf_claim(&self->data, sizeof(LogRow) + size);
	row->keys = keys;
	memset(&row->rows[0], 0, size);
	*key = log_row_of(self, op, prev);
}

hot static inline void
log_persist(Log* self, uint64_t partition)
{
	auto op = log_of(self, self->count - 1);
	// [cmd, partition, row]
	auto key = log_row_of(self, op, NULL);
	log_set_add(&self->log_set, op->cmd, partition, key->row);
}

static inline void
log_handle(Log*   self,
           LogCmd cmd,
           LogIf* iface,
           void*  iface_arg,
           void*  handle,
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
	ref->data   = data;

	// [cmd, data]
	log_set_add_op(&self->log_set, cmd, data);
}
