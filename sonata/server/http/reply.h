#pragma once

//
// sonata.
//
// SQL Database for JSON.
//

typedef struct Reply Reply;

struct Reply
{
	Buf header;
	Iov iov;
};

static inline void
reply_init(Reply* self)
{
	buf_init(&self->header);
	iov_init(&self->iov);
}

static inline void
reply_free(Reply* self)
{
	buf_free(&self->header);
	iov_free(&self->iov);
}

static inline void
reply_reset(Reply* self)
{
	buf_reset(&self->header);
	iov_reset(&self->iov);
}

static inline void
reply_create(Reply* self, int code, const char* msg, Buf* body)
{
	reply_reset(self);

	buf_printf(&self->header,
	           "HTTP/1.1 %d %s\r\n"
	           "Content-Type: application/json\r\n"
	           "Content-Length: %d\r\n"
	           "\r\n",
	           code, msg,
	           buf_size(body));

	iov_add_buf(&self->iov, &self->header);
	iov_add_buf(&self->iov, body);
}

static inline void
reply_write(Reply* self, Tcp* tcp)
{
	tcp_write(tcp, &self->iov);
}
