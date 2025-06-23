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

typedef struct Console Console;

struct Console
{
	bool           is_openned;
	bool           is_tty;
	int            fd_in;
	int            fd_out;
	int            cols;
	int            cursor;
	int            cursor_raw;
	Str*           prompt;
	int            prompt_len;
	Buf*           data;
	Buf*           refresh;
	struct termios term;
};

void console_init(Console*);
void console_free(Console*);
void console_save(Console*, Str*);
void console_load(Console*, Str*);
bool console(Console*, Str*, Str*);
