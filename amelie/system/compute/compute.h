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

typedef struct ComputeIf ComputeIf;
typedef struct Compute   Compute;

struct ComputeIf
{
	void* (*session_create)(Compute*, void*);
	void  (*session_free)(void*);
	bool  (*session_execute)(void*, Portal*, Request*);
};

struct Compute
{
	Clients    clients;
	ComputeIf* iface;
	void*      iface_arg;
	Task       task;
};

void compute_init(Compute*, ComputeIf*, void*);
void compute_free(Compute*);
void compute_start(Compute*);
void compute_stop(Compute*);
