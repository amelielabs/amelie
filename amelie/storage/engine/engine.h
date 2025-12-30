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

typedef struct Engine Engine;

struct Engine
{
	Service service;
	Vault*  vault;
	WalMgr* wal_mgr;
};

void engine_init(Engine*, WalMgr*, Vault*);
void engine_free(Engine*);
