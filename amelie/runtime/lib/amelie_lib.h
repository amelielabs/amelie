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

// data structures
#include "lib/rbtree.h"
#include "lib/hashtable.h"

// misc
#include "lib/misc.h"

// hashing
#include "lib/hash.h"
#include "lib/uuid.h"
#include "lib/crc.h"
#include "lib/base64.h"
#include "lib/base64url.h"

// utf8
#include "lib/utf8.h"

// time
#include "lib/timezone.h"
#include "lib/timezones.h"
#include "lib/date.h"
#include "lib/interval.h"
#include "lib/timestamp.h"

// types
#include "lib/vector.h"
#include "lib/decimal.h"
#include "lib/avg.h"

// generalized flags
#include "lib/flags.h"

// codec
#include "lib/codec.h"
#include "lib/codec_cache.h"
#include "lib/codec_compression.h"

// background workers
#include "lib/worker_req.h"
#include "lib/worker.h"
#include "lib/workers.h"

// logger
#include "lib/logger.h"

// cli
#include "lib/arg.h"
#include "lib/separator.h"
#include "lib/console.h"
#include "lib/histogram.h"

// csv
#include "lib/csv.h"
