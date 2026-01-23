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

// object
#include "object/object.h"

// object iterator
#include "object/reader.h"
#include "object/object_iterator.h"

// mapping
#include "object/mapping.h"

// merger
#include "object/merger.h"
