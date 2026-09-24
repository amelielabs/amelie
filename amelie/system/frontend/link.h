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

typedef struct Link Link;

enum
{
	LINK_ERROR,
	LINK_EXECUTE,
	LINK_FEED
};

struct Link
{
	Portal    portal;
	Request   req;
	Mcp       mcp;
	Json      json;
	Client*   client;
	Frontend* fe;
};

void link_init(Link*, Frontend*, Client*);
void link_free(Link*);
void link_main(Link*);

void frontend_link(Frontend*, Client*);
