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
#include "os/iov.h"
#include "os/fs.h"
#include "os/file.h"

// remote id
#include "os/remote.h"

// tls
#include "os/tls_lib.h"
#include "os/tls_context.h"
#include "os/tls.h"

// network io
#include "os/socket.h"
#include "os/tcp.h"
#include "os/readahead.h"
#include "os/listen.h"

// resolver
#include "os/resolver.h"

// os
#include "os/os.h"
