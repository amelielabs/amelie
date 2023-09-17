#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Main Main;

struct Main
{
	BufCache buf_cache;
	Logger   logger;
	UuidMgr  uuid_mgr;
	Resolver resolver;
	Config   config;
	Global   global;
	Task     task;
};

void main_init(Main*);
void main_free(Main*);
int  main_start(Main*, Str*, Str*);
void main_stop(Main*);
int  main_connect(Main*, Client*);
