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

bool cli_is_terminal(void);
void cli_open(char*);
void cli_close();
void cli_sync(char*);
bool cli(char*, Str*);
