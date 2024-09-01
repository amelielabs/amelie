#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Vars   Vars;

struct Vars
{
	List list;
	int  list_count;
};

void vars_init(Vars*);
void vars_free(Vars*);
void vars_define(Vars*, VarDef*);
void vars_set_data(Vars*, uint8_t**, bool);
void vars_set(Vars*, Str*, bool);
void vars_set_argv(Vars*, int, char**);
Buf* vars_list(Vars*, Vars*);
Buf* vars_list_persistent(Vars*);
Var* vars_find(Vars*, Str*);
void vars_print(Vars*);
