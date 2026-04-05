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
backup_send(Websocket* self, int op, Buf* data, uint64_t size)
{
	// [websocket header][backup header][data][payload]
	BackupHeader header =
	{
		.op_size = 0,
		.op      = op
	};
	auto         iov_count = 1;
	struct iovec iov[2];
	iov[0].iov_base = &header;
	iov[0].iov_len  = sizeof(header);
	if (data)
	{
		iov[1].iov_base = data->start;
		iov[1].iov_len  = buf_size(data);
		iov_count++;
		header.op_size  = iov[1].iov_len;
	}
	websocket_send(self, WS_BINARY, iov, iov_count, size);
}

static inline int
backup_recv(Websocket* self, Buf* buf)
{
	// [websocket header][backup header][op_data][data]
	BackupHeader header;
	websocket_recv(self, (uint8_t*)&header, sizeof(header));
	readahead_recv_buf(&self->client->readahead, buf, header.op_size);
	return header.op;
}
