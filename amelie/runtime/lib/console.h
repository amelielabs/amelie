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
	Str*           prompt;
	int            cols;
	Buf*           refresh;
	Buf*           buf;
	int            buf_pos;
    struct termios term;
};

void console_init(Console*);
void console_free(Console*);
void console_prepare(Console*, Str*);
void console_sync(Console*, Str*);
bool console(Console*, Str*, Str*);
