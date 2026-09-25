
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
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>

hot static inline int
link_sql(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto http     = &self->client->request;

	// check permission
	user_check(portal->user, PERM_SQL);

	// POST / (text/plain)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is_case(method, "POST", 4)))
		error("unsupported operation method");

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "text/plain", 10) &&
	    !str_is(content_type, "application/x-www-form-urlencoded", 33))
		error("unsupported operation content-type");

	// accept
	OutputIf* output_if;
	auto accept = &endpoint->accept.string;
	if (str_empty(accept) ||
	    str_is(accept, "*/*", 3) ||
	    str_is(accept, "text/plain", 10))
	{
		str_set(accept, "text/plain", 10);
		output_if = &output_text;
	} else
	if (str_is(accept, "application/json", 16)) {
		output_if = &output_json;
	} else {
		error("unsupported operation accept type");
	}
	output_set(&portal->output, endpoint, output_if, NULL);

	// set request
	auto req = &self->req;
	req->type = REQUEST_SQL;
	buf_str(&http->content, &req->text);
	return LINK_EXECUTE;
}

hot static inline int
link_api_mcp(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto http     = &self->client->request;

	// check permission
	user_check(portal->user, PERM_MCP);

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "application/json", 16))
		error("unsupported operation content-type");

	// accept (jsonrpc)
	auto accept = &endpoint->accept.string;
	if (!str_empty(accept) &&
	    !str_is(accept, "application/json", 16) &&
	    !str_is(accept, "*/*", 3))
		error("unsupported operation accept");

	str_set(accept, "application/json", 16);
	output_set(&portal->output, endpoint, &output_jsonrpc, NULL);

	Str content;
	buf_str(&http->content, &content);

	// parse mcp request and run commands which not
	// require execution
	auto mcp = &self->mcp;
	mcp_reset(mcp);
	if (! mcp_parse(mcp, &content, &self->req))
		return LINK_ERROR;

	return LINK_EXECUTE;
}

hot static inline int
link_api_get(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;

	// check FEED permission
	user_check(portal->user, PERM_FEED);

	// GET (text/event-stream) SSE
	auto content_type = &endpoint->content_type.string;
	str_set(content_type, "text/event-stream", 17);

	// accept (text/event-stream)
	auto accept = &endpoint->accept.string;
	if (!str_empty(accept) &&
	    !str_is(accept, "text/event-stream", 17) &&
	    !str_is(accept, "*/*", 3))
		error("unsupported operation accept");

	str_set(accept, "text/event-stream", 17);
	output_set(&portal->output, endpoint, &output_json, NULL);

	return LINK_FEED;
}

#if 0
hot static inline int
link_copy(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto http     = &self->client->request;

	// check permission
	user_check(portal->user, PERM_IMPORT);

	// POST /?copy=target (text/plain)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is_case(method, "POST", 4)))
		error("unsupported operation method");

	// content type
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "text/plain", 10) &&
	    !str_is(content_type, "text/csv", 8) &&
	    !str_is(content_type, "application/x-www-form-urlencoded", 33))
		error("unsupported operation content-type");

	// accept
	OutputIf* output_if;
	auto accept = &endpoint->accept.string;
	if (str_empty(accept) ||
	    str_is(accept, "*/*", 3)         ||
	    str_is(accept, "text/plain", 10) ||
	    str_is(accept, "text/csv", 8))
	{
		str_set(accept, "text/plain", 10);
		output_if = &output_text;
	} else
	if (str_is(accept, "application/json", 16)) {
		output_if = &output_json;
	} else {
		error("unsupported operation accept type");
	}
	output_set(&portal->output, endpoint, output_if, NULL);

	Str content;
	buf_str(&http->content, &content);

	// set request
	auto req = &self->req;
	req->type = REQUEST_COPY;
	req->args = str_u8(&content);
	req->args_size = str_size(&content);

	// set target
	auto target = opt_string_of(&portal->endpoint.copy);
	auto pos = target->pos;
	auto end = target->end;
	if (! portal_target(&pos, end, &req->rel_user, &req->rel))
		error("failed to read target");

	return LINK_EXECUTE;
}
#endif

hot static inline int
link_api(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto http     = &self->client->request;

	// check API permission
	user_check(portal->user, PERM_API);

	// find api
	auto uri = opt_string_of(&endpoint->endpoint);
	self->api = apis_find(&portal->user->config->apis, uri);
	if (! self->api)
		error("user {str}: api '{str}' not found",
		      &portal->user->config->name, uri);

	// GET /<user_api> (feed)
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(str_is_case(method, "GET", 3)))
		return link_api_get(self);

	// POST /<user_api> (application/json)
	if (unlikely(! str_is_case(method, "POST", 4)))
		error("unsupported method");

	// content type (json)
	auto content_type = &endpoint->content_type.string;
	if (!str_empty(content_type) &&
	    !str_is(content_type, "application/json", 16))
		error("unsupported operation content-type");

	// accept (json, text)
	OutputIf* output_if;
	auto accept = &endpoint->accept.string;

	if (str_empty(accept) ||
	    str_is(accept, "*/*", 3) ||
	    str_is(accept, "application/json", 16))
	{
		output_if = &output_json;
	} else
	if (str_is(accept, "text/plain", 10))
	{
		output_if = &output_text;
	} else {
		error("unsupported operation accept");
	}
	output_set(&portal->output, endpoint, output_if, NULL);

	Str content;
	buf_str(&http->content, &content);
	
	// parse json body
	auto json = &self->json;
	json_reset(json);
	json_parse(json, &content, NULL);

	// set request
	auto req = &self->req;
	req->type      = REQUEST_WRITE;
	req->rel_user  = self->api->rel_user;
	req->rel       = self->api->rel;
	req->args      = json->buf->start;
	req->args_size = buf_size(json->buf);
	return LINK_EXECUTE;
}

hot static inline int
link_root(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto http     = &self->client->request;

	// POST /
	auto method = &http->options[HTTP_METHOD];
	if (unlikely(! str_is_case(method, "POST", 4)))
		error("unsupported operation");

	if (opt_int_of(&endpoint->mcp))
		return link_api_mcp(self);

	return link_sql(self);
}

hot int
link_request(Link* self)
{
	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	auto output   = &portal->output;
	auto http     = &self->client->request;
	output_set_buf(output, &self->client->reply.content);

	// read endpoint and prepare request
	int  rc;
	auto on_error = error_catch
	(
		// parse request path and arguments
		uri_parse_request(endpoint, &http->options[HTTP_URL]);
		request_init(&self->req);

		// / or /<user_api>
		if (str_is(opt_string_of(&endpoint->endpoint), "/", 1))
			rc = link_root(self);
		else
			rc = link_api(self);
	);
	if (on_error)
	{
		rc = LINK_ERROR;
		if (output->iface)
			output_error(output, &am_self()->error);
	}

	// configure portal local
	portal_set_local(portal, true);
	output_set_local(output, &portal->local);
	return rc;
}
