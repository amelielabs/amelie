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

// source
#include "object/source.h"
#include "object/id.h"

// merge iterator
#include "object/merge_iterator.h"

// region
#include "object/region.h"
#include "object/region_iterator.h"
#include "object/region_writer.h"

// span (region index)
#include "object/span.h"
#include "object/span_iterator.h"
#include "object/span_writer.h"

// writer
#include "object/writer.h"

// object
#include "object/object.h"

// object iterator
#include "object/reader.h"
#include "object/object_iterator.h"

// merger
#include "object/merger.h"
