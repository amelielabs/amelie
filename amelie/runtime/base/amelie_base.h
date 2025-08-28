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

// compiler
#include "base/c.h"
#include "base/macro.h"
#include "base/overflow.h"

// os
#include "base/atomic.h"
#include "base/spinlock.h"
#include "base/mutex.h"
#include "base/cond_var.h"
#include "base/cond.h"
#include "base/thread.h"

// intrusive list
#include "base/list.h"

// exception
#include "base/exception.h"
#include "base/error.h"
#include "base/report.h"

// memory
#include "base/allocator.h"
#include "base/arena.h"

// string
#include "base/str.h"

// buffer manager
#include "base/buf.h"
#include "base/buf_cache.h"
#include "base/buf_list.h"
#include "base/buf_mgr.h"

// event loop
#include "base/timer_mgr.h"
#include "base/fd.h"
#include "base/poller.h"
#include "base/notify.h"

// cooperative multitasking
#include "base/event.h"
#include "base/context_stack.h"
#include "base/context.h"
#include "base/coroutine.h"
#include "base/coroutine_mgr.h"

// ipc
#include "base/wait.h"
#include "base/bus.h"
#include "base/msg.h"
#include "base/channel.h"
#include "base/task.h"

// runtime operations
#include "base/throw.h"
#include "base/defer.h"
#include "base/runtime.h"
#include "base/runtime_defer.h"

// rpc
#include "base/rpc.h"
#include "base/rpc_queue.h"
