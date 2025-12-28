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

typedef void (*LoadFunction)(Load*, Str*, Str*);

typedef enum
{
	LOAD_LINE
} LoadType;

struct Load
{
	LoadType     type;
	LoadFunction read;
	Buf          readahead;
	int          readahead_size;
	int          offset;
	uint64_t     offset_file;
	int          limit;
	int          limit_size;
	File         file;
};

void load_init(Load*);
void load_reset(Load*);
void load_free(Load*);
void load_open(Load*, LoadType, char*, int, int);
void load_close(Load*);
Buf* load_read(Load*, int*);
