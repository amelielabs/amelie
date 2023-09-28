#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct LogOpIf LogOpIf;
typedef struct LogOp   LogOp;
typedef struct Log     Log;

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

struct LogOpIf
{
	void (*abort)(void*, LogCmd, Row*, Row*);
};

struct LogOp
{
	LogType type;
	LogCmd  cmd;
	union
	{
		struct
		{
			Row*     row;
			Row*     prev;
			LogOpIf* iface;
			void*    iface_arg;
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
	Buf      data;
	Iov      data_iov;
	int      count;
	int      count_persistent;
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
	self->lsn              = 0;
	buf_reset(&self->op);
	buf_reset(&self->data);
	iov_reset(&self->data_iov);
	list_init(&self->link);
}

static inline void
log_reserve(Log* self, LogCmd cmd, Uuid* uuid)
{
	buf_reserve(&self->op, sizeof(LogOp));
	if (uuid)
	{
		buf_reserve(&self->data,
		            data_size_array(4) +
		            data_size_integer(cmd) +
		            data_size_integer(uuid->a) +
		            data_size_integer(uuid->b));
	} else
	{
		buf_reserve(&self->data,
		            data_size_array(2) +
		            data_size_integer(cmd));
	}
	iov_reserve(&self->data_iov, 2);
}

hot static inline void
log_add(Log*     self,
        LogCmd   cmd,
        LogOpIf* iface,
        void*    iface_arg,
        bool     persistent,
        Uuid*    uuid,
        Schema*  schema,
        Row*     row,
        Row*     prev)
{
	buf_reserve(&self->op, sizeof(LogOp));
	auto op = (LogOp*)self->op.position;
	op->type            = LOG_WRITE;
	op->cmd             = cmd;
	op->write.iface     = iface;
	op->write.iface_arg = iface_arg;
	op->write.row       = row;
	op->write.prev      = prev;
	buf_advance(&self->op, sizeof(LogOp));
	self->count++;
	if (! persistent)
		return;

	// [cmd, uuid a/b, row]
	int data_offset = buf_size(&self->data);
	int data_size;
	encode_array(&self->data, 4);
	encode_integer(&self->data, cmd);
	encode_integer(&self->data, uuid->a);
	encode_integer(&self->data, uuid->b);
	data_size = buf_size(&self->data) - data_offset;
	iov_add_buf(&self->data_iov, &self->data, data_offset, data_size);
	iov_add(&self->data_iov, row_data(row, schema), row_data_size(row, schema));
	self->count_persistent++;
}

static inline void
log_add_handle(Log*   self,
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

	// [cmd, data]
	int data_offset = buf_size(&self->data);
	int data_size;
	encode_array(&self->data, 2);
	encode_integer(&self->data, op->cmd);
	data_size = buf_size(&self->data) - data_offset;
	iov_add_buf(&self->data_iov, &self->data, data_offset, data_size);
	iov_add(&self->data_iov, data->start, buf_size(data));
	self->count_persistent++;
}
