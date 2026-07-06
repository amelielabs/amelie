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

typedef struct Replicas Replicas;

struct Replicas
{
	int  list_count;
	List list;
	Db*  db;
};

void replicas_init(Replicas*, Db*);
void replicas_free(Replicas*);
void replicas_open(Replicas*);
void replicas_start(Replicas*);
void replicas_stop(Replicas*);
void replicas_create(Replicas*, ReplicaConfig*, bool);
void replicas_drop(Replicas*, Uuid*, bool);
void replicas_list(Replicas*, Buf*, Uuid*, int);
Replica*
replicas_find(Replicas*, Uuid*);
