#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct ConfigLocal ConfigLocal;

struct ConfigLocal
{
	Var  timezone;
	Vars vars;
};

void config_local_init(ConfigLocal*);
void config_local_prepare(ConfigLocal*);
void config_local_free(ConfigLocal*);
