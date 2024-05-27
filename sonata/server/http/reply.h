#pragma once

//
// sonata.
//
// Real-Time SQL Database.
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
reply_create_header(Buf*        buf,
                    int         code,
                    const char* msg,
                    const char* content_type,
                    uint64_t    content_length)
{
	buf_printf(buf,
	           "HTTP/1.1 %d %s\r\n"
	           "Content-Type: %s\r\n"
	           "Content-Length: %" PRIu64 "\r\n"
	           "\r\n",
	           code, msg,
	           content_type,
	           content_length);
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
	tcp_write(tcp, iov_pointer(&self->iov), self->iov.iov_count);
}
