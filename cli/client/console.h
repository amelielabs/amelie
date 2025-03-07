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

bool console_is_terminal(void);
void console_open(char*);
void console_close();
void console_sync(char*);
bool console(char*, Str*);
