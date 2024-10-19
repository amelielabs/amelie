#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Import Import;

struct Import
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

void import_init(Import*, Share*, Dtr*);
void import_free(Import*);
void import_reset(Import*);
void import_prepare(Import*, Http*);
void import_run(Import*);
