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

typedef struct Main Main;

typedef enum
{
	MAIN_ERROR,
	MAIN_RUN,
	MAIN_COMPLETE
} MainRc;

struct Main
{
	Home     home;
	Instance instance;
	Task     task;
};

void   main_init(Main*);
void   main_free(Main*);
MainRc main_start(Main*, int, char**);
void   main_stop(Main*);
