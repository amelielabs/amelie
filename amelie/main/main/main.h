#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
//

typedef struct Main Main;

struct Main
{
	Logger      logger;
	Random      random;
	Resolver    resolver;
	Config      config;
	TimezoneMgr timezone_mgr;
	BufMgr      buf_mgr;
	Global      global;
};

void main_init(Main*);
void main_free(Main*);
void main_start(Main*);
void main_stop(Main*);
bool main_create(Main*, char*);
bool main_open(Main*, char*, int, char**);
