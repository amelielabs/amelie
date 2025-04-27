
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>
#include <amelie_system.h>

static inline void
system_metrics_net(System* self, Buf* buf)
{
	// {}
	unused(self);
	encode_obj(buf);

	// connections
	encode_raw(buf, "connections", 11);
	encode_integer(buf, opt_int_of(&state()->connections));

	// sent
	encode_raw(buf, "sent_bytes", 10);
	encode_integer(buf, opt_int_of(&state()->sent_bytes));

	// recv
	encode_raw(buf, "recv_bytes", 10);
	encode_integer(buf, opt_int_of(&state()->recv_bytes));

	encode_obj_end(buf);
}

static inline void
system_metrics_process(System* self, Buf* buf)
{
	// get memory usage
	uint64_t mem_virt     = 0;
	uint64_t mem_resident = 0;
	uint64_t mem_shared   = 0;
	os_memusage(&mem_virt, &mem_resident, &mem_shared);

	// get cpu usage
	int      cpu_count = 0;
	uint64_t cpu_usage;
	os_cpuusage_system(&cpu_count, &cpu_usage);

	// get cpu usage per worker
	auto  fe = &self->frontend_mgr;
	auto  be = &self->backend_mgr;

	int      workers = fe->workers_count + be->workers_count;
	int      workers_id[workers];
	uint64_t workers_usage[workers];
	int i = 0;
	for (; i < fe->workers_count; i++)
	{
		workers_id[i] = fe->workers[i].task.thread.tid;
		workers_usage[i] = 0;
	}
	for (int j = 0; j < be->workers_count; j++)
	{
		auto backend = be->workers[j];
		workers_id[i] = backend->task.thread.tid;
		workers_usage[i] = 0;
		i++;
	}
	os_cpuusage(workers, workers_id, workers_usage);

	// {}
	encode_obj(buf);

	// uptime
	encode_raw(buf, "uptime", 6);
	encode_integer(buf, 0); // todo

	// mem_virt
	encode_raw(buf, "mem_virt", 8);
	encode_integer(buf, mem_virt);

	// mem_resident
	encode_raw(buf, "mem_resident", 12);
	encode_integer(buf, mem_resident);

	// mem_shared
	encode_raw(buf, "mem_shared", 10);
	encode_integer(buf, mem_shared);

	// cpus
	encode_raw(buf, "cpu_count", 9);
	encode_integer(buf, cpu_count);

	// cpu
	encode_raw(buf, "cpu", 3);
	encode_integer(buf, cpu_usage);

	// cpu_frontends
	encode_raw(buf, "cpu_frontends", 13);
	encode_array(buf);
	i = 0;
	for (; i < fe->workers_count; i++)
		encode_integer(buf, workers_usage[i]);
	encode_array_end(buf);

	// cpu_backends
	encode_raw(buf, "cpu_backends", 12);
	encode_array(buf);
	auto j = 0;
	for (; j < be->workers_count; j++)
	{
		encode_integer(buf, workers_usage[i]);
		i++;
	}
	encode_array_end(buf);

	encode_obj_end(buf);
}

Buf*
system_metrics(System* self)
{
	auto buf = buf_create();

	// {}
	encode_obj(buf);

	// uuid
	encode_raw(buf, "uuid", 4);
	encode_string(buf, &config()->uuid.string);

	// version
	encode_raw(buf, "version", 7);
	encode_string(buf, &state()->version.string);

	// frontends
	encode_raw(buf, "frontends", 9);
	encode_integer(buf, opt_int_of(&config()->frontends));

	// backends
	encode_raw(buf, "backends", 8);
	encode_integer(buf, opt_int_of(&config()->backends));

	// db
	encode_raw(buf, "db", 2);
	auto db = db_status(&self->db);
	defer_buf(db);
	buf_write(buf, db->start, buf_size(db));

	// process
	encode_raw(buf, "process", 7);
	system_metrics_process(self, buf);

	// net
	encode_raw(buf, "net", 3);
	system_metrics_net(self, buf);

	// wal
	encode_raw(buf, "wal", 3);
	auto wal = wal_status(&self->db.wal_mgr.wal);
	defer_buf(wal);
	buf_write(buf, wal->start, buf_size(wal));

	// repl
	encode_raw(buf, "repl", 4);
	auto repl = repl_status(&self->repl);
	defer_buf(repl);
	buf_write(buf, repl->start, buf_size(repl));

	encode_obj_end(buf);
	return buf;
}
