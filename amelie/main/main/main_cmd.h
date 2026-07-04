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

typedef struct MainCmd MainCmd;

typedef void (*MainCmdFunction)(Main*);

struct MainCmd
{
	MainCmdFunction function;
	const char*     name;
	const char*     description;
};

extern MainCmd main_cmds[];
