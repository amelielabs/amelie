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

typedef struct BenchClientIf BenchClientIf;
typedef struct BenchClient   BenchClient;

struct BenchClientIf
{
	BenchClient* (*create)(void*);
	void         (*free)(BenchClient*);
	void         (*connect)(BenchClient*, Remote*);
	void         (*execute)(BenchClient*, Str*);
	void         (*import)(BenchClient*, Str*, Str*, Str*);
};

struct BenchClient
{
	BenchClientIf* iface;
};

static inline BenchClient*
bench_client_create(BenchClientIf* iface, void* arg)
{
	return iface->create(arg);
}

static inline void
bench_client_free(BenchClient* self)
{
	self->iface->free(self);
}

static inline void
bench_client_connect(BenchClient* self, Remote* remote)
{
	self->iface->connect(self, remote);
}

static inline void
bench_client_execute(BenchClient* self, Str* cmd)
{
	self->iface->execute(self, cmd);
}

static inline void
bench_client_import(BenchClient* self, Str* path, Str* content_type, Str* content)
{
	self->iface->import(self, path, content_type, content);
}

extern BenchClientIf bench_client_http;
