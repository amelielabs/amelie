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

typedef struct NodeMsg NodeMsg;

enum
{
	NODE_WRITE = 1,
	NODE_ACK   = 2
};

struct NodeMsg
{
	Uuid     id;
	uint64_t lsn;
	uint8_t  op;
	uint32_t size;
} packed;

static inline void
node_send(Client * self,
          int      op,
          Uuid*    id,
          uint64_t lsn,
          Buf*     data)
{
	// [msg][data]
	NodeMsg msg =
	{
		.id   = *id,
		.lsn  = lsn,
		.op   = op,
		.size = 0
	};
	auto         iov_count = 1;
	struct iovec iov[2];
	iov[0].iov_base = &msg;
	iov[0].iov_len  = sizeof(msg);
	if (data)
	{
		iov[1].iov_base = data->start;
		iov[1].iov_len  = buf_size(data);
		iov_count++;
		msg.size = iov[1].iov_len;
	}
	tcp_write(&self->tcp, iov, iov_count);
}

static inline bool
node_recv(Client* self, NodeMsg* msg, Buf* buf)
{
	auto readahead = &self->readahead;
	if (! readahead_recv(readahead, (uint8_t*)msg, sizeof(NodeMsg)))
		return false;
	if (buf)
		readahead_recv_buf(readahead, buf, msg->size);
	return true;
}
