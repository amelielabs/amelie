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

typedef struct DstEvent DstEvent;

enum
{
	DST_EVENT_REPLACE,
	DST_EVENT_DELETE,
	DST_EVENT_PUBLISH
};

struct DstEvent
{
	int    op;
	DstKey key;
};
