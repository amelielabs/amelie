#pragma once

//
// amelie.
//
// Real-Time SQL Database.
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
