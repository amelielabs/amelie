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

typedef struct Node Node;

typedef void (*NodeExecute)(Node*, NodeMsg*, Buf*);

struct Node
{
	Client*     client;
	NodeExecute execute;
	void*       execute_arg;
	Uuid        id_primary;
	Uuid        id_self;
	Recover*    recover;
};

void node_init(Node*, NodeExecute, void*, Recover*, Client*);
void node_main(Node*);
void node_validate(Node*, NodeMsg*);
