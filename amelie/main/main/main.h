#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Main Main;

struct Main
{
	Logger   logger;
	Random   random;
	Resolver resolver;
	Config   config;
	Global   global;
	Task     task;
};

void main_init(Main*);
void main_free(Main*);
int  main_start(Main*, Str*, Str*, Str*);
void main_stop(Main*);
