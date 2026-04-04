
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

#include <amelie_runtime>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_ws.h>

hot static inline int
ws_create(WsFrame* self, uint8_t header[14])
{
	// RFC6455 The WebSocket Protocol

	// 1 bit fin + 4 bits opcode
    auto pos = 0;
	header[pos++] = (self->fin ? 0x80 : 0x00) | (self->opcode & 0x0F);

	// 1 bit mask + 7 bit payload len
    uint8_t len;
	if (self->size <= 125)
		len = self->size;
	else
	if (self->size <= 0xFFFF)
		len = 126;
	else
		len = 127;
	header[pos++] = (self->mask ? 0x80 : 0x00) | len;

	// extended size
	auto size = self->size;
	switch (len) {
	case 126:
	{
		// 16 bit len (big-endian)
		header[pos++] = (uint8_t)(size >> 8);
		header[pos++] = (uint8_t)(size);
		break;
	}
	case 127:
	{
		// 64 bit len (big-endian)
		header[pos++] = (uint8_t)(size >> 56);
		header[pos++] = (uint8_t)(size >> 48);
		header[pos++] = (uint8_t)(size >> 40);
		header[pos++] = (uint8_t)(size >> 32);
		header[pos++] = (uint8_t)(size >> 24);
		header[pos++] = (uint8_t)(size >> 16);
		header[pos++] = (uint8_t)(size >> 8);
		header[pos++] = (uint8_t)(size);
		break;
	}
	default:
		// 7 bits
		break;
	}

	// 32 bits mask (big-endian, only if supplied)
	if (self->mask)
	{
		auto key = self->mask_key;
		header[pos++] = (uint8_t)(key >> 24);
		header[pos++] = (uint8_t)(key >> 16);
		header[pos++] = (uint8_t)(key >> 8);
		header[pos++] = (uint8_t)(key);
	}

	return pos;
}

void
ws_send(Client* client, WsFrame* frame, int op, bool mask, uint64_t size)
{
	// fill the frame
	frame->fin      = false;
	frame->opcode   = op;
	frame->mask     = mask;
	frame->mask_key = 0;
	frame->size     = size;
	if (mask)
		frame->mask_key = random_generate(&runtime()->random);
	
	// create and write websocket frame
	uint8_t header[14];
	auto    header_size = ws_create(frame, header);
	struct iovec iov =
	{
		.iov_base = header,
		.iov_len  = header_size
	};
	tcp_write(&client->tcp, &iov, 1);
}

void
ws_recv(Client* client, WsFrame* frame)
{
	// RFC6455 The WebSocket Protocol

	auto readahead = &client->readahead;
	uint8_t data[2];
	if (! readahead_recv(readahead, data, 2))
		error("ws: unexpected eof");

	// 1 bit fin + 4 bits opcode + 1 bit mask
	frame->fin    = (data[0] & 0x80) != 0;
	frame->opcode =  data[0] & 0x0F;
	frame->mask   = (data[1] & 0x80) != 0;

	// 7 bits len
	uint8_t len = data[1] & 0x7F;
	switch (len) {
	case 126:
	{
		// 16 bit len (big-endian)
		uint8_t size[2];
		if (! readahead_recv(readahead, size, 2))
			error("ws: unexpected eof");
		frame->size = ((uint64_t)size[0] << 8) | size[1];
		break;
	}
	case 127:
	{
		// 64 bit len (big-endian)
		uint8_t size[8];
		if (! readahead_recv(readahead, size, 8))
			error("ws: unexpected eof");
		frame->size = 
			((uint64_t)size[0] << 56) | ((uint64_t)size[1] << 48) |
			((uint64_t)size[2] << 40) | ((uint64_t)size[3] << 32) |
			((uint64_t)size[4] << 24) | ((uint64_t)size[5] << 16) |
			((uint64_t)size[6] << 8)  |  size[7];
		break;
	}
	default:
	{
		if (len <= 125)
		{
			frame->size = len;
			break;
		}
		error("ws: invalid frame length");
		break;
	}
	}

	// 32 bits mask (big-endian, only if supplied)
	if (frame->mask)
	{
		uint8_t mask[4];
		if (! readahead_recv(readahead, mask, 4))
			error("ws: unexpected eof");
		frame->mask_key = 
			((uint32_t)mask[0] << 24) | ((uint32_t)mask[1] << 16) |
			((uint32_t)mask[2] << 8)  |  mask[3];
	}
}
