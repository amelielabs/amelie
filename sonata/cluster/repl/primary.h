#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Primary Primary;

typedef void (*PrimaryReplay)(Primary*, Buf*);

struct Primary
{
	Client*       client;
	PrimaryReplay replay;
	void*         replay_arg;
};

void primary_init(Primary*, Client*, PrimaryReplay, void*);
bool primary_next(Primary*);
void primary_main(Primary*);
