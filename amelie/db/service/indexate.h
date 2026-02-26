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

typedef struct Indexate Indexate;

struct Indexate
{
	ServiceLock lock;
	Part*       origin;
	Index*      index;
	Table*      table;
	Service*    service;
};

void indexate_init(Indexate*, Service*);
void indexate_reset(Indexate*);
bool indexate_next(Indexate*, Table*, IndexConfig*);
