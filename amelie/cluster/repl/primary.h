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

typedef struct Primary Primary;

typedef void (*PrimaryReplay)(Primary*, Buf*);

struct Primary
{
	Client*       client;
	PrimaryReplay replay;
	void*         replay_arg;
	Recover*      recover;
};

void primary_init(Primary*, Recover*, Client*, PrimaryReplay, void*);
bool primary_next(Primary*);
void primary_main(Primary*);
