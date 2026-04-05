
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
#include <openssl/sha.h>

void
websocket_init(Websocket* self)
{
	self->client      = NULL;
	self->client_mode = false;
	iov_init(&self->iov);
	ws_init(&self->frame);
	str_init(&self->protocol);
}

void
websocket_free(Websocket* self)
{
	iov_free(&self->iov);
}

void
websocket_set(Websocket* self, Str* protocol, Client* client, bool client_mode)
{
	self->client      = client;
	self->client_mode = client_mode;
	str_set_str(&self->protocol, protocol);
}

static void
websocket_generate_key(Buf* self)
{
	// key = base64(rand)
	uint64_t rand[2];
	rand[0] = random_generate(&runtime()->random);
	rand[1] = random_generate(&runtime()->random);
	Str rand_str;
	str_set(&rand_str, (char*)&rand[0], sizeof(rand));
	base64_encode(self, &rand_str);
}

static void
websocket_generate_token(Buf* self, Str* key)
{
	// base64(sha1(key + magic))
	const char* magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	char concat[256];
	auto len = sfmt(concat, sizeof(concat), "%.*ss%s", str_size(key),
	                str_of(key), magic);
	unsigned char hash[20];
	SHA1((unsigned char*)concat, len, hash);
	Str hash_str;
	str_set(&hash_str, (char*)&hash, sizeof(hash));
	base64_encode(self, &hash_str);
}

void
websocket_connect(Websocket* self)
{
	auto client = self->client;

	// generate websocket key = base64(rand)
	Buf key;
	buf_init(&key);
	defer_buf(&key);
	websocket_generate_key(&key);

	// generate websocket token
	Buf token;
	buf_init(&token);
	defer_buf(&token);
	Str key_str;
	buf_str(&key, &key_str);
	websocket_generate_token(&token, &key_str);
	Str token_str;
	buf_str(&token, &token_str);

	// send
	auto request = &client->request;
	http_reset(request);
	auto buf = http_begin_request(request, client->endpoint, 0);
	buf_printf(buf, "Upgrade: websocket\r\n");
	buf_printf(buf, "Connection: Upgrade\r\n");
	buf_printf(buf, "Sec-WebSocket-Key: %.*s\r\n", buf_size(&key), buf_cstr(&key));
	buf_printf(buf, "Sec-WebSocket-Version: 13\r\n");
	buf_printf(buf, "Sec-WebSocket-Protocol: %.*s\r\n", str_size(&self->protocol),
	           str_of(&self->protocol));
	http_end(buf);
	tcp_write_buf(&client->tcp, buf);

	// read response
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("websocket: unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "101", 3))
		error("websocket: %.*s %.*s",
		      str_size(&reply->options[HTTP_CODE]),
		      str_of(&reply->options[HTTP_CODE]),
		      str_size(&reply->options[HTTP_MSG]),
		      str_of(&reply->options[HTTP_MSG]));

	// validate reply headers

	// Upgrade
	auto hdr = http_find(reply, "Upgrade", 7);
	if (!hdr || !str_is_case(&hdr->value, "websocket", 9))
		error("websocket: invalid upgrade header");

	// Connection
	hdr = http_find(reply, "Connection", 10);
	if (!hdr || !str_is_case(&hdr->value, "Upgrade", 7))
		error("websocket: invalid Connection header");

	// Sec-WebSocket-Accept
	hdr = http_find(reply, "Sec-WebSocket-Accept", 20);
	if (!hdr || !str_compare(&hdr->value, &token_str))
		error("websocket: invalid Sec-WebSocket-Accept header");

	// Sec-WebSocket-Protocol
	hdr = http_find(reply, "Sec-WebSocket-Protocol", 22);
	if (!hdr || !str_compare(&hdr->value, &self->protocol))
		error("websocket: invalid Sec-WebSocket-Protocol header");
}

void
websocket_accept(Websocket* self)
{
	auto client = self->client;

	// recv (already available)
	auto req = &client->request;

	// Upgrade
	auto hdr = http_find(req, "Upgrade", 7);
	if (!hdr || !str_is_case(&hdr->value, "websocket", 9))
		error("websocket: invalid upgrade header");

	// Connection
	hdr = http_find(req, "Connection", 10);
	if (!hdr || !str_is_case(&hdr->value, "Upgrade", 7))
		error("websocket: invalid Connection header");

	// Sec-WebSocket-Version
	hdr = http_find(req, "Sec-WebSocket-Version", 21);
	if (!hdr || !str_is(&hdr->value, "13", 2))
		error("websocket: invalid Sec-WebSocket-Version header");

	// Sec-WebSocket-Protocol
	hdr = http_find(req, "Sec-WebSocket-Protocol", 22);
	if (!hdr || !str_compare(&hdr->value, &self->protocol))
		error("websocket: invalid Sec-WebSocket-Protocol header");

	// Sec-WebSocket-Key
	auto key = http_find(req, "Sec-WebSocket-Key", 17);
	if (! key)
		error("websocket: invalid Sec-WebSocket-Key header");

	// generate websocket token
	Buf token;
	buf_init(&token);
	defer_buf(&token);
	websocket_generate_token(&token, &key->value);
	Str token_str;
	buf_str(&token, &token_str);

	// reply
	auto reply = &client->reply;
	http_reset(reply);
	auto buf = http_begin_reply(reply, client->endpoint, "101 Switching Protocols", 23, 0);
	buf_printf(buf, "Upgrade: websocket\r\n");
	buf_printf(buf, "Connection: Upgrade\r\n");
	buf_printf(buf, "Sec-WebSocket-Accept: %.*s\r\n", str_size(&token_str), token_str.pos);
	buf_printf(buf, "Sec-WebSocket-Protocol: %.*s\r\n", str_size(&self->protocol),
	           str_of(&self->protocol));
	http_end(buf);
	tcp_write_buf(&client->tcp, buf);
}

hot void
websocket_send(Websocket*    self, int op,
               struct iovec* data,
               int           data_count,
               uint64_t      size)
{
	// prepare frame
	auto frame = &self->frame;
	frame->fin      = false;
	frame->opcode   = op;
	frame->mask     = self->client_mode;
	frame->mask_key = 0;
	if (self->client_mode)
		frame->mask_key = random_generate(&runtime()->random);

	// additional size beside data (not included here)
	frame->size     = size;

	// add frame with data to iov
	auto iov = &self->iov;
	iov_reset(iov);
	iov_add(iov, frame->header, 0);
	if (data)
	{
		for (auto i = 0; i < data_count; i++)
		{
			iov_add(iov, data[i].iov_base, data[i].iov_len);
			frame->size += data[i].iov_len;
		}
	}

	// create websocket frame
	ws_create(frame);

	// update frame size
	iov_pointer(iov)->iov_len = frame->header_size;

	// send frame with data
	tcp_write(&self->client->tcp, iov_pointer(iov), iov->iov_count);
}

bool
websocket_recv(Websocket* self, uint8_t* data, int data_size)
{
	// [websocket header][data]
	if (! ws_recv(&self->frame, self->client))
		return false;
	if (data_size > 0)
		readahead_recv(&self->client->readahead, data, data_size);
	return true;
}
