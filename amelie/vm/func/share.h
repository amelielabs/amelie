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

typedef struct Share Share;

struct Share
{
	Gtrs*      gtrs;
	Commit*    commit;
	Repl*      repl;
	Cdc*       cdc;
	Functions* functions;
	Db*        db;
	RecoverIf* recover_if;
	void*      recover_if_arg;
};

#define share() ((Share*)am_share)
