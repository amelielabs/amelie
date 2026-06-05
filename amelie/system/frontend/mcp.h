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

typedef struct Query Query;
typedef struct Mcp   Mcp;

typedef enum
{
	MCP_UNDEF,
	MCP_INITIALIZE,
	MCP_TOOLS_LIST,
	MCP_TOOLS_CALL,
	MCP_RESOURCES_LIST,
	MCP_RESOURCES_READ
} McpType;

struct Mcp
{
	McpType  type;
	Str      rel_user;
	Str      rel;
	uint8_t* args;
	Request* request;
	Jsonrpc  jsonrpc;
};

void mcp_init(Mcp*, Request*);
void mcp_free(Mcp*);
void mcp_reset(Mcp*);
bool mcp_parse(Mcp*, Str*, Query*);
