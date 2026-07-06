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
node_send(Websocket* self,
          int        op,
          Uuid*      id,
          uint64_t   lsn,
          Buf*       data)
{
	// [websocket header][msg][data]
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
	websocket_send(self, WS_BINARY, iov, iov_count, 0);
}

static inline bool
node_recv(Websocket* self, NodeMsg* msg, Buf* buf)
{
	// [websocket header][msg][data]
	if (! websocket_recv(self, (uint8_t*)msg, sizeof(*msg)))
		return false;
	if (buf)
		readahead_recv_buf(&self->client->readahead, buf, msg->size);
	return true;
}
