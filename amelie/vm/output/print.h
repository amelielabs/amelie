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

typedef struct PrintCol PrintCol;
typedef struct Print    Print;

struct PrintCol
{
	Column* column;
	int     size_header;
	int     size_value;
	int     size;
	bool    pretty;
};

struct Print
{
	Buf       cols;
	int       cols_count;
	Columns*  columns;
	Value*    value;
	int       size;
	int       size_max;
	Buf*      buf;
	Str       chr_cut;
	Str       chr_line;
	Timezone* tz;
};

static inline PrintCol*
print_at(Print* self, int order)
{
	return &((PrintCol*)self->cols.start)[order];
}

void print_init(Print*);
void print_free(Print*);
void print_reset(Print*);
void print_create(Print*, Columns*, Value*, Timezone*, Buf*);
void print_run(Print*);
