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

typedef struct ConfigLocal ConfigLocal;

struct ConfigLocal
{
	Var  timezone;
	Var  format;
	// testing
	Var  test_bool;
	Var  test_int;
	Var  test_string;
	Var  test_json;
	Vars vars;
};

void config_local_init(ConfigLocal*);
void config_local_prepare(ConfigLocal*);
void config_local_free(ConfigLocal*);
