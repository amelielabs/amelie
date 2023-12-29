
//
// indigo
//
// SQL OLTP database
//

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>

#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <assert.h>
#include <math.h>

#include <indigo.h>
#include "histogram.h"

typedef volatile uint64_t atomic_u64_t;

static inline uint32_t
atomic_u64_of(atomic_u64_t *atomic)
{
	return __sync_fetch_and_add(atomic, 0);
}

static inline uint32_t
atomic_u64_add(atomic_u64_t *atomic, int value)
{
	return __sync_fetch_and_add(atomic, value);
}

/*
static inline uint64_t
atomic_u64_set(atomic_u64_t *atomic, int value)
{
	return __sync_lock_test_and_set(atomic, value);
}
*/

typedef struct
{
	int          id;
	pthread_t    thread;
	atomic_u64_t count;
	atomic_u64_t count_last;
	Histogram    histogram;
} Worker;

static volatile int  writers_run;
static Worker*       writers;
static int           writers_count;               
static volatile int  readers_run;
static Worker*       readers;
static int           readers_count;               
static volatile int  report_run;
static pthread_t     report_thread;

static atomic_u64_t  count_rd;
static atomic_u64_t  count_wr;
static atomic_u64_t  count_rd_data;
static atomic_u64_t  count_wr_data;

static indigo_t*       mm;

static inline void
print_object(indigo_object_t *object, int deep)
{
	indigo_arg_t  data;
	indigo_type_t type;
	if (! indigo_next(object, &data, &type))
		return;

	switch (type) {
	case INDIGO_REAL:
		printf("%f", *(double*)data.data);
		break;
	case INDIGO_BOOL:
		if (*(int8_t*)data.data > 0)
			printf("true");
		else
			printf("false");
		break;
	case INDIGO_INT:
		printf("%" PRIi64, *(int64_t*)data.data);
		break;
	case INDIGO_STRING:
		printf("\"%.*s\"", data.data_size, (char*)data.data);
		break;
	case INDIGO_NULL:
		printf("null");
		break;
	case INDIGO_ARRAY:
	{
		int count = *(int64_t*)data.data;
		printf("[ ");
		while (count-- > 0)
		{
			print_object(object, deep);
			if (count > 0)
				printf(", ");
		}
		printf(" ]");
		break;
	}
	case INDIGO_MAP:
	{
		int i = 0;
		int count = *(int64_t*)data.data;
		if (count == 0) {
			printf("{ }");
			break;
		}
		printf("{\n");
		while (count-- > 0)
		{
			for (i = 0; i < deep + 1; i++)
				printf("  ");

			/* key */
			print_object(object, deep + 1);
			printf(": ");

			/* value */
			print_object(object, deep + 1);

			if (count > 0)
				printf(",\n");
			else
				printf("\n");
		}
		for (i = 0; i < deep; i++)
			printf("  ");
		printf("}");
		break;
	}
	}
}

static inline void
print_error(indigo_object_t *object, int verbose)
{
	/* map */
	indigo_type_t type;
	indigo_arg_t  data;
	indigo_next(object, &data, &type);
	if (type != INDIGO_MAP && data.data_size != 2)
		goto error;

	/* code */
	indigo_next(object, &data, &type);
	if (type != INDIGO_STRING)
		goto error;
	indigo_next(object, &data, &type);
	if (type != INDIGO_INT)
		goto error;

	/* msg */
	indigo_next(object, &data, &type);
	if (type != INDIGO_STRING)
		goto error;
	indigo_next(object, &data, &type);
	if (type != INDIGO_STRING)
		goto error;

	char *msg = data.data;
	int   msg_len = data.data_size;
	printf("error: %.*s\n", msg_len, msg);
	return;

error:
	printf("error: bad error reply\n");
}

static indigo_session_t*
client_connect(indigo_t *env, char *uri)
{
	indigo_session_t *session = NULL;
	while (! session)
	{
		session = indigo_connect(env, uri);
		if (session == NULL)
		{
			printf("error: indigo_connect() to '%s' failed\n", uri);
			continue;
		}

		bool active = true;
		while (active)
		{
			indigo_object_t *result = NULL;
			indigo_event_t event;
			event = indigo_read(session, -1, &result);
			switch (event) {
			case INDIGO_CONNECT:
				active  = false;
				break;
			case INDIGO_DISCONNECT:
				active  = false;
				indigo_free(session);
				session = NULL;
				printf("error: failed to connect to '%s'\n", uri);
				sleep(1);
				break;
			case INDIGO_ERROR:
				if (result)
					print_error(result, 0);
				break;
			default:
				assert(0);
			}
			if (result)
				indigo_free(result);
		}
	}

	return session;
}

static indigo_session_t*
client_execute(indigo_t *env, indigo_session_t *session, char *uri, char *command)
{
	bool connected = true;
	for (;;)
	{
		if (! connected)
		{
			session = client_connect(env, uri);
			assert(session != NULL);
			connected = true;
		}

		int rc;
		rc = indigo_execute(session, command, 0, NULL);
		if (rc == -1)
		{
			printf("error: indigo_execute() failed\n");
			connected = false;
			indigo_free(session);
			continue;
		}

		bool active = true;
		while (active)
		{
			indigo_object_t *result = NULL;
			indigo_event_t event;
			event = indigo_read(session, -1, &result);
			switch (event) {
			case INDIGO_DISCONNECT:
				active = false;
				connected = false;
				indigo_free(session);
				session = NULL;
				printf("disconnected\n");
				break;
			case INDIGO_ERROR:
				if (result)
					print_error(result, 0);
				break;
			case INDIGO_OBJECT:
				print_object(result, 0);
				printf("\n");
				break;
			case INDIGO_OK:
				active = false;
				break;
			default:
				assert(0);
			}
			if (result)
				indigo_free(result);
		}

		if (connected)
			break;
	}

	return session;
}

#if 0
void *writer_main(void *arg)
{
	Worker *worker = arg;

	char command[64 * 1024];
	int  len = 0;

	/*snprintf(uri_str, sizeof(uri_str), "indigo@127.0.0.1:3485/main");*/

	char *uri = NULL;
	/*char *uri ="indigo@127.0.0.1:3485/main";*/
	indigo_session_t *session = client_connect(mm, uri);

	srand(0);
	while (writers_run)
	{
		len = snprintf(command, sizeof(command), "REPLACE INTO test VALUES ");
		/*len = snprintf(command, sizeof(command), "SELECT ");*/

		int count = 1000;
		int i = 0;
		while (i < count)
		{
			uint64_t value = atomic_u64_add(&count_wr, 1);
			atomic_u64_add(&worker->count, 1);

#if 0
			atomic_u64_add(&count_wr, 1);
			uint64_t value = rand();
			atomic_u64_add(&worker->count, 1);
#endif

			len += snprintf(command + len, sizeof(command) - len,
			                "(%d)%s", (int)value, i != (count-1) ? "," : "");
			i++;
		}
		atomic_u64_add(&count_wr_data, len - 17);


#if 0
		double t0 = histogram_time();

		session = client_execute(mm, session, uri, command);
		/*session = client_execute(mm, session, uri, " ");*/

		double t1 = histogram_time();
		double tb = t1 - t0;
		histogram_add(&worker->histogram, tb);
#endif
		session = client_execute(mm, session, uri, command);



#if 0
		int rc;
		rc = indigo_execute(session, command, 0, NULL);
		if (rc == -1)
		{
			printf("error\n");
			continue;
		}
		indigo_event_t event;
		event = indigo_read(session, 0, NULL);
		if (event == INDIGO_ERROR)
			printf("error\n");
#endif
	}

	indigo_free(session);
	return NULL;
}
#endif

void *writer_main(void *arg)
{
	Worker *worker = arg;

	int count_sessions = 10;
	indigo_session_t *sessions[count_sessions];
	for (int i = 0; i < count_sessions; i++)
		sessions[i] = client_connect(mm, NULL);

	/*char* cmd = "REPLACE INTO test generate 500";*/
	/*char* cmd = "INSERT INTO test generate 500 ON CONFLICT DO NOTHING";*/
	/*char* cmd = "INSERT INTO test generate 500 ON CONFLICT DO UPDATE SET data = data + 1";*/
	char* cmd = "INSERT INTO test generate 500 ON CONFLICT DO UPDATE SET data = data + 1";

	/*char* cmd = "INSERT INTO test generate 500 ON CONFLICT DO UPDATE SET data = data + 1 WHERE data + data = 0";*/

	srand(0);
	while (writers_run)
	{
		for (int i = 0; i < count_sessions; i++)
		{
			atomic_u64_add(&count_wr, 500);
			atomic_u64_add(&worker->count, 500);

			int rc;
			rc = indigo_execute(sessions[i], cmd, 0, NULL);
			if (rc == -1)
			{
				printf("error: indigo_execute() failed\n");
				continue;
			}
		}

		for (int i = 0; i < count_sessions; i++)
		{
			bool active = true;
			while (active)
			{
				indigo_object_t *result = NULL;
				indigo_event_t event;
				event = indigo_read(sessions[i], -1, &result);
				switch (event) {
				case INDIGO_DISCONNECT:
					printf("disconnected\n");
					break;
				case INDIGO_ERROR:
					if (result)
						print_error(result, 0);
					break;
				case INDIGO_OBJECT:
					print_object(result, 0);
					printf("\n");
					break;
				case INDIGO_OK:
					active = false;
					break;
				default:
					assert(0);
				}
				if (result)
					indigo_free(result);
			}
		}
	}

	for (int i = 0; i < count_sessions; i++)
		indigo_free(sessions[i]);

	return NULL;
}

void *reader_main(void *arg)
{
	Worker *worker = arg;

	/*char *uri = "127.0.0.1:3485";*/
	char *uri = NULL;
	/*char *uri ="indigo@127.0.0.1:3485/main";*/
	indigo_session_t *session = client_connect(mm, uri);

	srand(0);
	while (readers_run)
	{
		char command[128];

		snprintf(command, sizeof(command), "SELECT * FROM test WHERE id >= %d LIMIT 100",
			(int)(rand() % 2000000));

		/*len = snprintf(command, sizeof(command), "SELECT * FROM test WHERE id = %d",*/
		/*(int)(rand() %   2000000));*/

		/*
		len = snprintf(command, sizeof(command), "SELECT * FROM [1, 2, 3, 4, 5, 6, 7, 8, 9] test WHERE * = %d",
		               (int)(rand() % 10));
					   */

		/*
		len = snprintf(command, sizeof(command), "SELECT * FROM test WHERE id >= %d LIMIT 100",
		               rand());
					   */

		/*
		len = snprintf(command, sizeof(command), "SELECT * FROM test WHERE id = %d", rand());
		*/
		/*
		len = snprintf(command, sizeof(command), "SELECT 1");
		(void)len;
		*/

		int rc;
		rc = indigo_execute(session, command, 0, NULL);
		if (rc == -1)
		{
			printf("error\n");
			continue;
		}

		for (;;)
		{
			indigo_object_t *object;

			indigo_event_t event;
			event = indigo_read(session, -1, &object);
			if (event == INDIGO_OK)
				break;
			if (event == INDIGO_ERROR)
				printf("error\n");

			atomic_u64_add(&count_rd, 1);
			atomic_u64_add(&worker->count, 1);

			if (object)
			{
				/*print_object(object, 0);*/
				indigo_free(object);
				object = NULL;
			}
		}
	}

	indigo_free(session);

	return NULL;
}

void *report_main(void *arg)
{
	uint64_t last_rd      = atomic_u64_of(&count_rd);
	uint64_t last_wr      = atomic_u64_of(&count_wr);
	uint64_t last_rd_data = atomic_u64_of(&count_rd_data);
	uint64_t last_wr_data = atomic_u64_of(&count_wr_data);

	char *uri = NULL;
	indigo_session_t *session = client_connect(mm, uri);

	while (report_run)
	{
		sleep(1);

		/*
		session = client_execute(mm, session, uri, "show stores");
		histogram_print(&writers[0].histogram);
		*/
		printf("\n");

		int i;
		for (i = 0; i < readers_count; i++)
		{
			uint64_t value = readers[i].count;
			printf("reader[%d]: %d rps\n", i, (int)(value - readers[i].count_last));
			readers[i].count_last = value;
		}

		for (i = 0; i < writers_count; i++)
		{
			uint64_t value = writers[i].count;
			printf("writer[%d]: %d rps\n", i, (int)(value - writers[i].count_last));
			writers[i].count_last = value;
		}

		uint64_t crd  = atomic_u64_of(&count_rd);
		uint64_t crdd = atomic_u64_of(&count_rd_data);
		uint64_t cwr  = atomic_u64_of(&count_wr);
		uint64_t cwrd = atomic_u64_of(&count_wr_data);

		printf("read:  %d rps (%.2f Mbs)\n", (int)crd - (int)last_rd,
		       (crdd - last_rd_data)  / 1024.0 / 1024.0);

		printf("write: %d rps (%.2f Mbs)\n", (int)cwr - (int)last_wr,
		       (cwrd - last_wr_data)  / 1024.0 / 1024.0);


		last_rd      = atomic_u64_of(&count_rd);
		last_wr      = atomic_u64_of(&count_wr);
		last_rd_data = atomic_u64_of(&count_rd_data);
		last_wr_data = atomic_u64_of(&count_wr_data);

		printf("\n");
		printf("\n");
		fflush(stdout);
	}

	indigo_free(session);
	return NULL;
}

extern uint64_t timer_mgr_gettime(void);

static void
bench(void)
{
	int count_sessions = 6;
	int count = 10000000 * count_sessions;

	indigo_session_t *sessions[count_sessions];
	for (int i = 0; i < count_sessions; i++)
		sessions[i] = client_connect(mm, NULL);

	uint64_t time = timer_mgr_gettime();

	for (int i = 0; i < count_sessions; i++)
	{
		int rc;
		rc = indigo_execute(sessions[i], "show be", 0, NULL);
		if (rc == -1)
		{
			printf("error: indigo_execute() failed\n");
			continue;
		}
	}

	for (int i = 0; i < count_sessions; i++)
	{
		bool active = true;
		while (active)
		{
			indigo_object_t *result = NULL;
			indigo_event_t event;
			event = indigo_read(sessions[i], -1, &result);
			switch (event) {
			case INDIGO_DISCONNECT:
				printf("disconnected\n");
				break;
			case INDIGO_ERROR:
				if (result)
					print_error(result, 0);
				break;
			case INDIGO_OBJECT:
				print_object(result, 0);
				printf("\n");
				break;
			case INDIGO_OK:
				active = false;
				break;
			default:
				assert(0);
			}
			if (result)
				indigo_free(result);
		}
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	printf("%f rps\n", rps);

	for (int i = 0; i < count_sessions; i++)
		indigo_free(sessions[i]);
}

static void
bench_file(Worker* worker)
{
	int count = 50000000;
	int batch = 500;

	int fd = open("/home/pmwkaa/studio/temp/sql_gen/test", 0644);
	if (fd == -1)
	{
		printf("failed to open test file\n");
		return;
	}

	struct stat st;
	fstat(fd, &st);

	char* start = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
	char* pos = start;



	int count_sessions = 10;
	indigo_session_t *sessions[count_sessions];
	for (int i = 0; i < count_sessions; i++)
		sessions[i] = client_connect(mm, NULL);

	/*char* cmd = "REPLACE INTO test generate 500 1";*/

	uint64_t time = timer_mgr_gettime();

	srand(0);

	int i = 0;
	while (i < (count / batch))
	{
		for (int j = 0; j < count_sessions; j++)
		{
			int len = *(int*)pos;
			pos += sizeof(int);
			char* cmd = pos;

			atomic_u64_add(&count_wr, 500);
			atomic_u64_add(&worker->count, 500);

			int rc;
			rc = indigo_execute(sessions[j], cmd, 0, NULL);
			if (rc == -1)
			{
				printf("error: indigo_execute() failed\n");
				continue;
			}
			
			pos += len;
			i++;
		}

		for (int j = 0; j < count_sessions; j++)
		{
			bool active = true;
			while (active)
			{
				indigo_object_t *result = NULL;
				indigo_event_t event;
				event = indigo_read(sessions[j], -1, &result);
				switch (event) {
				case INDIGO_DISCONNECT:
					printf("disconnected\n");
					break;
				case INDIGO_ERROR:
					if (result)
						print_error(result, 0);
					break;
				case INDIGO_OBJECT:
					print_object(result, 0);
					printf("\n");
					break;
				case INDIGO_OK:
					active = false;
					break;
				default:
					assert(0);
				}
				if (result)
					indigo_free(result);
			}
		}
	}

	time = (timer_mgr_gettime() - time) / 1000;

	// rps
	double rps = count / (float)(time / 1000.0 / 1000.0);
	printf("%f rps\n", rps);

	for (int i = 0; i < count_sessions; i++)
		indigo_free(sessions[i]);

	munmap(start, st.st_size);
	close(fd);
}

int
main(int argc, char *argv[])
{
	readers_count = 1;
	readers = malloc(sizeof(Worker) * readers_count);
	memset(readers, 0, sizeof(Worker) * readers_count);

	writers_count = 3;
	writers = malloc(sizeof(Worker) * writers_count);
	memset(writers, 0, sizeof(Worker) * writers_count);

	int i;
	for (i = 0; i < readers_count; i++)
	{
		readers[i].id = i;
		histogram_init(&readers[i].histogram);
	}
	for (i = 0; i < writers_count; i++)
	{
		writers[i].id = i;
		histogram_init(&writers[i].histogram);
	}

	mm = indigo_init();

	char options[] = "{ \"log_to_stdout\": true, \"listen\": [{ \"host\": \"*\", \"port\": 3485 }], \"cluster_shards\": 10, \"cluster_hubs\": 3}";
	int rc;
	rc = indigo_open(mm, "./_test", options);
	if (rc == -1)
	{
		indigo_free(mm);
		return 1;
	}

	indigo_session_t* session = client_connect(mm, NULL);

#if 0
	session = client_execute(mm, session, uri, "create user if not exists indigo");
	/*session = client_execute(mm, session, &uri, "create table if not exists test (id int primary key) with (compression = \"zstd\")");*/

	session = client_execute(mm, session, uri, "create tier if not exists hot (in_memory = true, capacity = 10)");
	session = client_execute(mm, session, uri, "create tier if not exists warm (in_memory = true, compression=\"zstd\", capacity = 20)");
	session = client_execute(mm, session, uri, "create tier if not exists cold (in_memory = false, compression=\"zstd\")");

	session = client_execute(mm, session, uri,
		                     "create table if not exists test (id int primary key) tiering(hot, warm, cold)");

	/*
	session = client_execute(mm, session, uri, "create storage if not exists a (in_memory = true, snapshot=true)");
	session = client_execute(mm, session, uri, "create storage if not exists b (in_memory = true, snapshot=true)");
	session = client_execute(mm, session, uri,
		                     "create table if not exists test (id int primary key) "
							 " tier default(storage=\"a, b\") ");
							 */
#endif
	/*session = client_execute(mm, session, NULL, "create table if not exists test (id int primary key serial)");*/
	/*session = client_execute(mm, session, NULL, "create table if not exists test (id int primary key serial, a int default 0, b int default 0, c int default 0, d int default 0, e int default 0, f int default 0, g int default 0, h int default 0, k int default 0)");*/

	/*session = client_execute(mm, session, NULL, "create table if not exists test (id int primary key serial, data int default 0)");*/
	/*session = client_execute(mm, session, NULL, "create table if not exists test (id int primary key serial, data int default 0) partition by (id) interval 10000000");*/
	session = client_execute(mm, session, NULL, "create table if not exists test (id int primary key serial, data int default 0)");

	for (;;)
	{
		printf("> ");
		fflush(stdout);

		char command[512];
		char* p = fgets(command, sizeof(command), stdin);
		if (p == NULL)
			break;

		if (! strcmp(command, "/start_report\n"))
		{
			if (report_run)
			{
				printf("error: already active\n");
				continue;
			}

			report_run = 1;
			pthread_create(&report_thread, NULL, report_main, NULL);

		} else
		if (! strcmp(command, "/stop_report\n"))
		{
			if (! report_run)
			{
				printf("error: not active\n");
				continue;
			}

			report_run = 0;
			pthread_join(report_thread, NULL);

		} else
		if (! strcmp(command, "/bench\n"))
		{
			bench();
		} else
		if (! strcmp(command, "/bench_file\n"))
		{
			/*
			report_run = 1;
			pthread_create(&report_thread, NULL, report_main, NULL);
			*/

			bench_file(&writers[0]);
		} else
		if (! strcmp(command, "/start\n"))
		{
			if (writers_run)
			{
				printf("error: already active\n");
				continue;
			}

			writers_run = 1;
			for (i = 0; i < writers_count; i++)
				pthread_create(&writers[i].thread, NULL, writer_main, &writers[i]);

			if (! report_run)
			{
				report_run = 1;
				pthread_create(&report_thread, NULL, report_main, NULL);
			}

		} else
		if (! strcmp(command, "/stop\n"))
		{
			if (report_run)
			{
				report_run = 0;
				pthread_join(report_thread, NULL);
			}

			if (! writers_run)
			{
				printf("error: not active\n");
				continue;
			}
			writers_run = 0;
			for (i = 0; i < writers_count; i++)
				pthread_join(writers[i].thread, NULL);

		} else
		if (! strcmp(command, "/start_rd\n"))
		{
			if (readers_run)
			{
				printf("error: already active\n");
				continue;
			}
			readers_run = 1;
			for (i = 0; i < readers_count; i++)
				pthread_create(&readers[i].thread, NULL, reader_main, &readers[i]);

			if (! report_run)
			{
				report_run = 1;
				pthread_create(&report_thread, NULL, report_main, NULL);
			}

		} else
		if (! strcmp(command, "/stop_rd\n"))
		{
			if (! readers_run)
			{
				printf("error: not active\n");
				continue;
			}
			readers_run = 0;
			for (i = 0; i < readers_count; i++)
				pthread_join(readers[i].thread, NULL);

			if (report_run && !writers_run)
			{
				report_run = 0;
				pthread_join(report_thread, NULL);
			}

		} else
		{
			session = client_execute(mm, session, NULL, command);
		}
	}

	if (readers_run)
	{
		readers_run = 0;
		for (i = 0; i < readers_count; i++)
			pthread_join(readers[i].thread, NULL);
	}

	if (writers_run)
	{
		writers_run = 0;
		for (i = 0; i < writers_count; i++)
			pthread_join(writers[i].thread, NULL);
	}

	if (report_run)
	{
		report_run = 0;
		pthread_join(report_thread, NULL);
	}

	indigo_free(session);
	indigo_free(mm);
	return 0;
}
