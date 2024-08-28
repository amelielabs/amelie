#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Ctl Ctl;

typedef enum
{
	CTL_START,
	CTL_STOP,
	CTL_COMMON
} CtlType;

struct Ctl
{
	CtlType type;
	bool    daemon;
	char*   directory;
	int     directory_fd;
	int     argc;
	char**  argv;
};

void ctl_init(Ctl*);
int  ctl_main(Ctl*, int, char**);
void ctl_wait_for_signal(void);
