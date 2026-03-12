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

// action
#include "service/action.h"
#include "service/action_cache.h"
#include "service/action_waiter.h"
#include "service/action_mgr.h"

// service lock manager
#include "service/service_lock_mgr.h"

// service file
#include "service/service_file.h"

// service
#include "service/service.h"
#include "service/service_recover.h"

// syncer
#include "service/syncer.h"

// service operations
#include "service/refresh.h"
#include "service/op.h"

// create index
#include "service/indexate.h"
#include "service/op_create_index.h"
