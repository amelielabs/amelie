#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Reader Reader;

struct Reader
{
	Buf      readahead;
	int      readahead_size;
	int      offset;
	uint64_t offset_file;
	int      limit;
	int      limit_size;
	File     file;
};

void reader_init(Reader*);
void reader_reset(Reader*);
void reader_free(Reader*);
void reader_open(Reader*, const char*, int, int);
void reader_close(Reader*);
Buf* reader_read(Reader*, int*);
