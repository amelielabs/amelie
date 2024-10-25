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

// partition mapping
#include "partition/router.h"
#include "partition/part_map.h"

// partition
#include "partition/part_config.h"
#include "partition/part.h"
#include "partition/part_build.h"
#include "partition/part_op.h"
#include "partition/part_mgr.h"
#include "partition/part_list.h"
