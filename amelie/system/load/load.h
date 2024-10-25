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

typedef struct Load Load;

struct Load
{
	Buf         columns;
	bool        columns_has;
	int         columns_count;
	Table*      table;
	ReqList     req_list;
	Dtr*        dtr;
	Json        json;
	HttpHeader* content_type;
	Http*       request;
	Uri         uri;
	Share*      share;
};

void load_init(Load*, Share*, Dtr*);
void load_free(Load*);
void load_reset(Load*);
void load_prepare(Load*, Http*);
void load_run(Load*);
