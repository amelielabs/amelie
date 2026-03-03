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

// service lock manager
#include "service/service_lock_mgr.h"

// service file
#include "service/service_file.h"

// service
#include "service/service.h"
#include "service/service_recover.h"

// service operations
#include "service/refresh.h"
#include "service/indexate.h"
#include "service/flush.h"
#include "service/split.h"
#include "service/op.h"
