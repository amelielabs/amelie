#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct CommandArgPtr CommandArgPtr;
typedef struct Command       Command;

struct CommandArgPtr
{
	void* data;
	int   data_size;
};

struct Command
{
	Str  text;
	int  argc;
	Buf  argv;
	Buf* request;
};

static inline void
command_init(Command* self)
{
	self->argc    = 0;
	self->request = NULL;
	str_init(&self->text);
	buf_init(&self->argv);
}

static inline void
command_free(Command* self)
{
	buf_free(&self->argv);
}

static inline uint8_t*
command_arg(Command* self, int n)
{
	assert(n < self->argc);
	return msg_of(self->request)->data + buf_u32(&self->argv)[n];
}

static inline void
command_set(Command* self, Buf* buf)
{
	buf_reset(&self->argv);

	// read request
	auto pos = msg_of(buf)->data;
	int count;
	data_read_array(&pos, &count);
	data_read_string(&pos, &self->text);
	data_read_array(&pos, &self->argc);

	// indexate arguments
	buf_reserve(&self->argv, sizeof(uint32_t) * self->argc);
	auto index = buf_u32(&self->argv);
	for (int i = 0; i < self->argc; i++)
	{
		index[i] = pos - msg_of(buf)->data;
		data_skip(&pos);
	}
	self->request = buf;
}

static inline Buf*
command_create(BufCache*      buf_cache,
               Str*           text,
               int            argc,
               CommandArgPtr* argv)
{
	// create message
	auto buf = msg_create_nothrow(buf_cache, MSG_COMMAND, 0);
	if (unlikely(buf == NULL))
		return NULL;

	// [text, [args]]

	// array 
	int rc;
	rc = buf_reserve_nothrow(buf, data_size_array(2));
	if (unlikely(rc == -1))
		goto error;
	data_write_array(&buf->position, 2);

	// text
	rc = buf_reserve_nothrow(buf, data_size_string(str_size(text)));
	if (unlikely(rc == -1))
		goto error;
	data_write_string(&buf->position, text);

	// array
	rc = buf_reserve_nothrow(buf, data_size_array(argc));
	if (unlikely(rc == -1))
		goto error;
	data_write_array(&buf->position, argc);

	for (int i = 0; i < argc; i++)
	{
		rc = buf_reserve_nothrow(buf, data_size_string(argv[i].data_size));
		if (unlikely(rc == -1))
			goto error;
		data_write_raw(&buf->position, argv[i].data, argv[i].data_size);
	}

	msg_end(buf);
	return buf;

error:
	buf_cache_push(buf_cache, buf);
	return NULL;
}
