#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Runner Runner;

typedef enum
{
	RUNNER_START,
	RUNNER_STOP,
	RUNNER_COMMON
} RunnerType;

struct Runner
{
	RunnerType type;
	bool       daemon;
	char*      directory;
	int        directory_fd;
	int        argc;
	char**     argv;
};

void runner_init(Runner*);
int  runner_main(Runner*, int, char**);
void runner_wait_for_signal(void);
