
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
#include <amelie_jsonrpc.h>

void
jsonrpc_init(Jsonrpc* self)
{
	self->reqs_count = 0;
	buf_init(&self->reqs);
	buf_init(&self->data);
	json_init(&self->parser);
}

void
jsonrpc_free(Jsonrpc* self)
{
	buf_free(&self->reqs);
	buf_free(&self->data);
	json_free(&self->parser);
}

void
jsonrpc_reset(Jsonrpc* self)
{
	self->reqs_count = 0;
	buf_reset(&self->reqs);
	buf_reset(&self->data);
	json_reset(&self->parser);
}

hot static bool
jsonrpc_parse_obj(Jsonrpc* self, uint8_t** pos)
{
	// create new request
	auto req = (JsonrpcReq*)buf_emplace(&self->reqs, sizeof(JsonrpcReq));
	req->id     = NULL;
	req->params = NULL;
	str_init(&req->method);

	// parse object
	json_read_obj(pos);
	while (! json_read_obj_end(pos))
	{
		Str name;
		Str value;
		json_read_string(pos, &name);
		if (str_is(&name, "jsonrpc", 7))
		{
			if (unlikely(! json_is_string(*pos)))
				return false;
			json_read_string(pos, &value);
			if (unlikely(! str_is(&value, "2.0", 3)))
				return false;
		} else
		if (str_is(&name, "method", 6))
		{
			if (unlikely(! json_is_string(*pos)))
				return false;
			json_read_string(pos, &req->method);
		} else
		if (str_is(&name, "params", 6))
		{
			req->params = *pos;
			json_skip(pos);
		} else
		if (str_is(&name, "id", 2))
		{
			if (unlikely(!json_is_string(*pos) && !json_is_integer(*pos)))
				return false;
			req->id = *pos;
			json_skip(pos);
		} else {
			return false;
		}
	}

	// ensure method and id are set
	if (str_empty(&req->method) || !req->id)
		return false;

	self->reqs_count++;
	return true;
}

hot static inline bool
jsonrpc_parse_request(Jsonrpc* self)
{
	auto buf = &self->data;
	if (unlikely(buf_empty(buf)))
		return false;

	auto pos = buf->start;
	if (json_is_obj(pos))
		return jsonrpc_parse_obj(self, &pos);

	if (json_is_array(pos))
	{
		while (! json_read_array_end(&pos))
			if (! jsonrpc_parse_obj(self, &pos))
				return false;
		return true;
	}

	return false;
}

hot void
jsonrpc_parse(Jsonrpc* self, Str* text)
{
	json_parse(&self->parser, text, &self->data);
	if (! jsonrpc_parse_request(self))
		error("jsonrpc: invalid request");
}
