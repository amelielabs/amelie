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

// retention
#include "page/retention.h"

// page
#include "page/page.h"
#include "page/page_mgr.h"
#include "page/pages.h"

// compression
#include "page/encoder.h"
#include "page/basefile.h"

// checkpoint
#include "page/checkpoint.h"
#include "page/checkpoints.h"
