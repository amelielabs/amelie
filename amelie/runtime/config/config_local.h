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
	List list;
};

void config_local_init(ConfigLocal*);
void config_local_prepare(ConfigLocal*);
void config_local_free(ConfigLocal*);
Var* config_local_find(ConfigLocal*, Str*);
