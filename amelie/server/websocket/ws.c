
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
#include <amelie_websocket.h>

hot void
ws_create(Ws* self)
{
	// RFC6455 The WebSocket Protocol
	auto header = self->header;

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

	// 32 bits mask (only if supplied)
	if (self->mask)
	{
		auto key = self->mask_key;
		header[pos++] = key[0];
		header[pos++] = key[1];
		header[pos++] = key[2];
		header[pos++] = key[3];
	}

	self->header_size = pos;
}

bool
ws_recv(Ws* self, Client* client)
{
	// RFC6455 The WebSocket Protocol
	auto readahead = &client->readahead;
	uint8_t data[2];
	if (! readahead_recv(readahead, data, 2))
		return false;

	// 1 bit fin + 4 bits opcode + 1 bit mask
	self->fin    = (data[0] & 0x80) != 0;
	self->opcode =  data[0] & 0x0F;
	self->mask   = (data[1] & 0x80) != 0;

	// 7 bits len
	uint8_t len = data[1] & 0x7F;
	switch (len) {
	case 126:
	{
		// 16 bit len (big-endian)
		uint8_t size[2];
		if (! readahead_recv(readahead, size, 2))
			error("websocket: unexpected eof");
		self->size = ((uint64_t)size[0] << 8) | size[1];
		break;
	}
	case 127:
	{
		// 64 bit len (big-endian)
		uint8_t size[8];
		if (! readahead_recv(readahead, size, 8))
			error("ws: unexpected eof");
		self->size =
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
			self->size = len;
			break;
		}
		error("websocket: invalid frame length");
		break;
	}
	}

	// 32 bits mask (only if supplied)
	if (self->mask)
	{
		if (! readahead_recv(readahead, self->mask_key, 4))
			error("websocket: unexpected eof");
	}
	return true;
}
