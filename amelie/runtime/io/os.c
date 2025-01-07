
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

void
os_memusage(uint64_t* virt, uint64_t* resident, uint64_t* shared)
{
	auto buf = buf_create();
	defer_buf(buf);
	file_import_stream(buf, "/proc/self/statm");
	buf_write(buf, "\0", 1);

	unsigned long stat_size;
	unsigned long stat_resident;
	unsigned long stat_shared;
	auto rc = sscanf(buf_cstr(buf), "%ld %ld %ld", &stat_size,
	                 &stat_resident, &stat_shared);
	if (rc != 3)
		error("failed to read fields from /proc/self/statm");

	auto page_size = getpagesize();
	*virt = stat_size * page_size;
	*resident = stat_resident * page_size;
	*shared = stat_shared * page_size;
}

void
os_cpuusage_system(int* cpus, uint64_t* usage)
{
	// get total cpu usage

	// /proc/stat
	auto buf = buf_create();
	defer_buf(buf);
	file_import_stream(buf, "/proc/stat");
	buf_write(buf, "\0", 1);

	unsigned long long stat_user;
	unsigned long long stat_nice;
	unsigned long long stat_system;
	unsigned long long stat_idle;
	auto rc = sscanf(buf_cstr(buf), "%*s %llu %llu %llu %llu",
	                 &stat_user,
	                 &stat_nice,
	                 &stat_system, &stat_idle);
	if (rc != 4)
		error("failed to read fields from /proc/stat");

	*cpus = sysconf(_SC_NPROCESSORS_ONLN);
    *usage = stat_user + stat_nice + stat_system + stat_idle;
}

void
os_cpuusage(int count, int* ids, uint64_t* usage)
{
	auto buf = buf_create();
	defer_buf(buf);

	// /proc/id/stat
	auto pid = getpid();
	for (int i = 0; i < count; i++)
	{
		buf_reset(buf);
		file_import_stream(buf, "/proc/%d/task/%d/stat", pid, ids[i]);
		buf_write(buf, "\0", 1);

		unsigned long stat_usertime;
		unsigned long stat_systime;
		auto rc = sscanf(buf_cstr(buf), "%*d %*s %*c %*d "
		                 "%*d %*d %*d %*d %*u %*lu %*lu %*lu %*lu "
		                 "%lu %lu",
		                 &stat_usertime, &stat_systime);
		if (rc != 2)
			error("failed to read fields from /proc/id/stat");

    	usage[i] = stat_usertime + stat_systime;
	}
}
