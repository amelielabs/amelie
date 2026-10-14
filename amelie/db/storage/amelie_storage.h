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

// storage manifest
#include "storage/manifest.h"

// storage
#include "storage/page.h"
#include "storage/retention.h"
#include "storage/storage.h"

// compression
#include "storage/encoder.h"
#include "storage/basefile.h"
