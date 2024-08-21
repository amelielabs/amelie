#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct MainOptions MainOptions;
typedef struct Main        Main;

enum
{
	MAIN_DIRECTORY,
	MAIN_CONFIG,
	MAIN_BACKUP,
	MAIN_BACKUP_CAFILE,
	MAIN_MAX
};

struct MainOptions
{
	Str options[MAIN_MAX];
};

struct Main
{
	Logger      logger;
	Random      random;
	Resolver    resolver;
	Config      config;
	TimezoneMgr timezone_mgr;
	Global      global;
	Task        task;
};

void main_init(Main*);
void main_free(Main*);
int  main_start(Main*, Str*);
void main_stop(Main*);
