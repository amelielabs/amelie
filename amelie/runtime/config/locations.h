#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Locations Locations;

struct Locations
{
	List list;
	int  list_count;
};

void locations_init(Locations*);
void locations_free(Locations*);
void locations_open(Locations*, const char*);
void locations_sync(Locations*, const char*);
void locations_add(Locations*, Remote*);
void locations_delete(Locations*, Str*);
Remote*
locations_find(Locations*, Str*);
