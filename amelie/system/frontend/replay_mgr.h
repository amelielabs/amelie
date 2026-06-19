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

typedef struct ReplayMgr ReplayMgr;

struct ReplayMgr
{
	Replay*    replay;
	int        replay_count;
	uint64_t   replay_id;
	int        rr;
	ReplaySync sync;
};

ReplayMgr*
replay_mgr_allocate();
void replay_mgr_free(ReplayMgr*);
void replay_mgr_start(ReplayMgr*, FrontendMgr*);
void replay_mgr_stop(ReplayMgr*);
void replay_mgr_sync(ReplayMgr*);
void replay_mgr_execute(ReplayMgr*, RecordMsg*);
