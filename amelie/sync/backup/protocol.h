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

typedef struct BackupHeader BackupHeader;

enum
{
	BACKUP_JOIN = 1,
	BACKUP_PULL = 2,
	BACKUP_PUSH = 3,
	BACKUP_DONE = 4
};

struct BackupHeader
{
	uint16_t op_size;
	uint8_t  op;
} packed;

static inline void
backup_connect(Client* self)
{
	// do websocket handshake
	Str protocol;
	str_set(&protocol, "amelie-v1-backup", 16);
	ws_connect(self, &protocol);
}

static inline void
backup_accept(Client* self)
{
	// do websocket handshake
	Str protocol;
	str_set(&protocol, "amelie-v1-backup", 16);
	ws_accept(self, &protocol);
}

static inline void
backup_send(Client*  self, WsFrame* frame,
            bool     mask,
            int      op,
            Buf*     op_data,
            uint64_t size)
{
	// [websocket header][backup header][op_data][data]
	BackupHeader header =
	{
		.op_size = 0,
		.op      = op
	};
	auto         iov_count = 1;
	struct iovec iov[2];
	iov[0].iov_base = &header;
	iov[0].iov_len  = sizeof(header);
	if (op_data)
	{
		iov[1].iov_base = op_data->start;
		iov[1].iov_len  = buf_size(op_data);
		iov_count++;
		header.op_size  = iov[1].iov_len;
	}
	ws_send(self, frame, WS_BINARY, mask,
	        sizeof(BackupHeader) + header.op_size + size);
	tcp_write(&self->tcp, iov, iov_count);
}

static inline int
backup_recv(Client* self, WsFrame* frame, Buf* buf)
{
	// [websocket header][backup header][op_data][data]
	ws_recv(self, frame);
	if (frame->opcode != WS_BINARY)
		return -1;
	BackupHeader header;
	readahead_recv(&self->readahead, (uint8_t*)&header, sizeof(header));
	readahead_recv_buf(&self->readahead, buf, header.op_size);
	return header.op;
}
