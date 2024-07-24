#pragma once

//
// amelie.
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
	Recover*      recover;
};

void primary_init(Primary*, Recover*, Client*, PrimaryReplay, void*);
bool primary_next(Primary*);
void primary_main(Primary*);
