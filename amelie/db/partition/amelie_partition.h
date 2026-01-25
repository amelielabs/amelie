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

// partition
#include "partition/part.h"
#include "partition/part_op.h"

// partition manager
#include "partition/part_mapping.h"
#include "partition/part_mgr.h"
#include "partition/part_mgr_recover.h"

// deploy
#include "partition/deploy.h"

// global partition locking
#include "partition/part_lock_mgr.h"
