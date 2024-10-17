#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

bool cli_is_terminal(void);
void cli_open(char*);
void cli_sync(char*);
bool cli(char*, Str*);
