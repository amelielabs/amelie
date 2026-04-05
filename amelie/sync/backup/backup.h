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

typedef struct Backup Backup;

struct Backup
{
	DbSnapshot* snapshot;
	Db*         db;
	Websocket   websocket;
	Event       on_complete;
	Task        task;
};

void backup_init(Backup*, Db*);
void backup_free(Backup*);
void backup_run(Backup*, Client*);
void backup(Db*, Client*);
