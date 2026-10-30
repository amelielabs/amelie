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
	LINK_STREAM
};

struct Link
{
	Portal   portal;
	Request  req;
	Mcp      mcp;
	Json     json;
	Api*     api;
	Client*  client;
	Compute* compute;
};

Task* compute_link(Compute*, Client*);
