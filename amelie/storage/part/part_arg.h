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

typedef struct PartArg PartArg;

struct PartArg
{
	Rel*       rel;
	Uuid*      rel_id;
	Usage*     memory;
	Columns*   columns;
	Retention* retention;
	PageMgr*   page_mgr;
};
