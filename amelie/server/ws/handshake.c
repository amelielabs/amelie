
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

#include <openssl/sha.h>

static void
ws_generate_key(Buf* self)
{
	// key = base64(rand)
	uint64_t rand = random_generate(&runtime()->random);
	Str rand_str;
	str_set(&rand_str, (char*)&rand, sizeof(rand));
	base64_encode(self, &rand_str);
}

static void
ws_generate_token(Buf* self, Str* key)
{
	// base64(sha1(key + magic))
	const char* magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	char concat[256];
	auto len = sfmt(concat, sizeof(concat), "%.*ss%s", str_size(key),
	                str_of(key), magic);
	unsigned char hash[20];
	SHA1((unsigned char*)concat, len, hash);
	Str hash_str;
	str_set(&hash_str, (char*)&hash, sizeof(rand));
	base64_encode(self, &hash_str);
}

void
ws_connect(Client* self, Str* protocol)
{
	// generate websocket key = base64(rand)
	Buf key;
	buf_init(&key);
	defer_buf(&key);
	ws_generate_key(&key);

	// generate websocket token
	Buf token;
	buf_init(&token);
	defer_buf(&token);
	Str key_str;
	buf_str(&key, &key_str);
	ws_generate_token(&token, &key_str);
	Str token_str;
	buf_str(&token, &token_str);

	// send
	auto request = &self->request;
	auto buf = http_begin_request(request, self->endpoint, 0);
	buf_printf(buf, "Upgrade: websocket\r\n");
	buf_printf(buf, "Connection: Upgrade\r\n");
	buf_printf(buf, "Sec-WebSocket-Key: %.*s\r\n", buf_size(&key), buf_cstr(&key));
	buf_printf(buf, "Sec-WebSocket-Version: 13\r\n");
	buf_printf(buf, "Sec-WebSocket-Protocol: %.*s\r\n", str_size(protocol),
	           str_of(protocol));
	http_end(buf);
	tcp_write_buf(&self->tcp, buf);

	// read response
	auto reply = &self->reply;
	http_reset(reply);
	auto eof = http_read(reply, &self->readahead, false);
	if (eof)
		error("unexpected eof");
	if (! str_is(&reply->options[HTTP_CODE], "101", 3))
		error("unexpected reply code");

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
	if (!hdr || !str_compare(&hdr->value, protocol))
		error("websocket: invalid Sec-WebSocket-Protocol header");
}

void
ws_accept(Client* self, Str* protocol)
{
	// recv (already available)
	auto req = &self->request;

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
	if (!hdr || !str_compare(&hdr->value, protocol))
		error("websocket: invalid Sec-WebSocket-Protocol header");

	// Sec-WebSocket-Key
	auto key = http_find(req, "Sec-WebSocket-Key", 17);
	if (! key)
		error("websocket: invalid Sec-WebSocket-Key header");

	// generate websocket token
	Buf token;
	buf_init(&token);
	defer_buf(&token);
	ws_generate_token(&token, &key->value);
	Str token_str;
	buf_str(&token, &token_str);

	// reply
	auto reply = &self->reply;
	auto buf = http_begin_reply(reply, self->endpoint, "101 Switching Protocols", 23, 0);
	buf_printf(buf, "Upgrade: websocket\r\n");
	buf_printf(buf, "Connection: Upgrade\r\n");
	buf_printf(buf, "Sec-WebSocket-Accept: %.*s\r\n", str_size(&token_str), token_str.pos);
	buf_printf(buf, "Sec-WebSocket-Protocol: %.*s\r\n", str_size(protocol),
	           str_of(protocol));
	http_end(buf);
	tcp_write_buf(&self->tcp, buf);
}
