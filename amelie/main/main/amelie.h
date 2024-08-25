#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Amelie Amelie;

typedef enum
{
	AMELIE_ERROR,
	AMELIE_RUN,
	AMELIE_COMPLETE
} AmelieRc;

struct Amelie
{
	Home home;
	Main main;
	Task task;
};

void     amelie_init(Amelie*);
void     amelie_free(Amelie*);
AmelieRc amelie_start(Amelie*, int, char**);
void     amelie_stop(Amelie*);
