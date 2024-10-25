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

// file io
#include "io/iov.h"
#include "io/vfs.h"
#include "io/fs.h"
#include "io/file.h"

// file reader
#include "io/reader.h"

// os
#include "io/os.h"

// remote id
#include "io/remote.h"

// tls
#include "io/tls_lib.h"
#include "io/tls_context.h"
#include "io/tls.h"

// network io
#include "io/socket.h"
#include "io/poll.h"
#include "io/tcp.h"
#include "io/readahead.h"
#include "io/listen.h"

// resolver
#include "io/resolver.h"
