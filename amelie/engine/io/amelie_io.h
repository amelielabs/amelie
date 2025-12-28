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
#include "io/source.h"
#include "io/id.h"

// merge iterator
#include "io/merge_iterator.h"

// region
#include "io/region.h"
#include "io/region_iterator.h"
#include "io/region_writer.h"

// span (region index)
#include "io/span.h"
#include "io/span_iterator.h"
#include "io/span_writer.h"
#include "io/span_op.h"

// writer
#include "io/writer.h"

// partition
#include "io/part.h"

// reader
#include "io/reader.h"

// partition iterator
#include "io/part_iterator.h"

// merger
/*#include "io/merger.h"*/
