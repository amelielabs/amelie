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

struct Node
{
	uint64_t  coroutine_id;
	Websocket websocket;
	Client*   client;
	Recover   recover;
	List      link;
};

void node_init(Node*, Db*, RecoverIf*, void*, Client*);
void node_free(Node*);
void node_main(Node*);
