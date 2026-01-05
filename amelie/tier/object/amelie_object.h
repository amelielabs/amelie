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

// metadata (object index)
#include "object/meta.h"
#include "object/meta_iterator.h"
#include "object/meta_writer.h"

// writer
#include "object/hasher.h"
#include "object/writer.h"

// object
#include "object/object_file.h"
#include "object/object.h"

// object iterator
#include "object/reader.h"
#include "object/object_iterator.h"

// merger
#include "object/merger.h"
