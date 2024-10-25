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

typedef struct Reader Reader;

typedef void (*ReaderFn)(Reader*, Str*, Str*);

typedef enum
{
	READER_LINE
} ReaderType;

struct Reader
{
	ReaderType type;
	ReaderFn   read;
	Buf        readahead;
	int        readahead_size;
	int        offset;
	uint64_t   offset_file;
	int        limit;
	int        limit_size;
	File       file;
};

void reader_init(Reader*);
void reader_reset(Reader*);
void reader_free(Reader*);
void reader_open(Reader*, ReaderType, char*, int, int);
void reader_close(Reader*);
Buf* reader_read(Reader*, int*);
