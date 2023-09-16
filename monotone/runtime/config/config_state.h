#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct ConfigState ConfigState;

struct ConfigState
{
	File  file;
	Mutex write_lock;
};

void config_state_init(ConfigState*);
void config_state_free(ConfigState*);
void config_state_open(ConfigState*, Config*, const char*);
void config_state_close(ConfigState*);
void config_state_save(ConfigState*, Config*);
