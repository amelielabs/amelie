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

// merge iterator
#include "object/merge_iterator.h"

// region
#include "object/region.h"
#include "object/region_iterator.h"
#include "object/region_writer.h"

// metadata (object index)
#include "object/meta.h"
#include "object/meta_iterator.h"
#include "object/meta_writer.h"

// writer
#include "object/writer.h"

// branch
#include "object/branch.h"

// branch iterator
#include "object/reader.h"
#include "object/branch_iterator.h"

// object
#include "object/object.h"
#include "object/mapping.h"
