#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct Compact Compact;

struct Compact
{
	Task task;
};

void compact_init(Compact*);
void compact_free(Compact*);
void compact_start(Compact*);
void compact_stop(Compact*);
void compact_run(Compact*, CompactReq*);
