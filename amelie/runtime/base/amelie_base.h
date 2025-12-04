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

// report
#include "base/report.h"

// intrusive list
#include "base/list.h"

// memory
#include "base/allocator.h"
#include "base/arena.h"

// string
#include "base/fmt.h"
#include "base/str.h"

// buffer manager
#include "base/buf.h"
#include "base/buf_cache.h"
#include "base/buf_mgr.h"

// exception
#include "base/exception.h"
#include "base/error.h"

// event loop
#include "base/timer_mgr.h"
#include "base/fd.h"
#include "base/poller.h"
#include "base/notify.h"

// event
#include "base/ipc.h"
#include "base/event.h"

// cooperative multitasking
#include "base/context_stack.h"
#include "base/context.h"
#include "base/coroutine.h"
#include "base/coroutine_mgr.h"
#include "base/wait.h"

// bus
#include "base/ring.h"
#include "base/msg.h"
#include "base/mailbox.h"
#include "base/bus.h"

// task
#include "base/task_log.h"
#include "base/task.h"

// runtime operations
#include "base/throw.h"
#include "base/defer.h"
#include "base/runtime.h"
#include "base/runtime_defer.h"

// rpc
#include "base/rpc.h"
#include "base/rpc_queue.h"
