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

typedef struct TimezoneMgr TimezoneMgr;

struct TimezoneMgr
{
	Hashtable ht;
	Timezone* system;
};

void timezone_mgr_init(TimezoneMgr*);
void timezone_mgr_free(TimezoneMgr*);
void timezone_mgr_open(TimezoneMgr*);
Timezone*
timezone_mgr_find(TimezoneMgr*, Str*);
