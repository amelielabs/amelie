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
	Table*  table;
	Buf     columns;
	bool    columns_has;
	int     columns_count;
	ReqList req_list;
	Dtr*    dtr;
	Http*   request;
	Json    json;
	Uri     uri;
	Local*  local;
	Share*  share;
};

void import_init(Import*, Share*, Local*);
void import_free(Import*);
void import_reset(Import*);
void import_prepare(Import*, Dtr*, Http*);
void import_run(Import*);
