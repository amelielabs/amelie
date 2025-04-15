
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

#include <amelie.h>
#include <amelie_cli.h>
#include <amelie_cli_client.h>

static inline TopStats*
top_stats_allocate(void)
{
	auto self = (TopStats*)am_malloc(sizeof(TopStats));
	memset(self, 0, sizeof(TopStats));
	return self;
}

static inline void
top_stats_free(TopStats* self)
{
	str_free(&self->uuid);
	str_free(&self->version);
	buf_free(&self->cpu_frontends);
	buf_free(&self->cpu_backends);
	buf_free(&self->repl);
	am_free(self);
}

static inline void
top_stats_read(TopStats* self, uint8_t** pos)
{
	self->time_us = time_us();

	uint8_t* pos_db      = NULL;
	uint8_t* pos_process = NULL;
	uint8_t* pos_net     = NULL;
	uint8_t* pos_wal     = NULL;
	uint8_t* pos_repl    = NULL;
	Decode obj[] =
	{
		{ DECODE_STRING, "uuid",              &self->uuid      },
		{ DECODE_STRING, "version",           &self->version   },
		{ DECODE_INT,    "frontends",         &self->frontends },
		{ DECODE_INT,    "backends",          &self->backends  },
		{ DECODE_OBJ,    "db",                &pos_db          },
		{ DECODE_OBJ,    "process",           &pos_process     },
		{ DECODE_OBJ,    "net",               &pos_net         },
		{ DECODE_OBJ,    "wal",               &pos_wal         },
		{ DECODE_OBJ,    "repl",              &pos_repl        },
		{ 0,              NULL,               NULL             },
	};
	decode_obj(obj, "stats", pos);

	// db
	Decode obj_db[] =
	{
		{ DECODE_INT,    "schemas",           &self->schemas           },
		{ DECODE_INT,    "tables",            &self->tables            },
		{ DECODE_INT,    "secondary_indexes", &self->secondary_indexes },
		{ 0,              NULL,               NULL                     },
	};
	decode_obj(obj_db, "db", &pos_db);

	// process
	uint8_t* pos_cpu_frontends = NULL;
	uint8_t* pos_cpu_backends  = NULL;
	Decode obj_process[] =
	{
		{ DECODE_INT,    "uptime",            &self->uptime       },
		{ DECODE_INT,    "mem_virt",          &self->mem_virt     },
		{ DECODE_INT,    "mem_resident",      &self->mem_resident },
		{ DECODE_INT,    "mem_shared",        &self->mem_shared   },
		{ DECODE_INT,    "cpu_count",         &self->cpu_count    },
		{ DECODE_INT,    "cpu",               &self->cpu          },
		{ DECODE_ARRAY,  "cpu_frontends",     &pos_cpu_frontends  },
		{ DECODE_ARRAY,  "cpu_backends",      &pos_cpu_backends   },
		{ 0,              NULL,               NULL                },
	};
	decode_obj(obj_process, "process", &pos_process);

	json_read_array(&pos_cpu_frontends);
	while (! json_read_array_end(&pos_cpu_frontends))
	{
		int64_t value;
		json_read_integer(&pos_cpu_frontends, &value);
		buf_write(&self->cpu_frontends, &value, sizeof(int64_t));
	}

	json_read_array(&pos_cpu_backends);
	while (! json_read_array_end(&pos_cpu_backends))
	{
		int64_t value;
		json_read_integer(&pos_cpu_backends, &value);
		buf_write(&self->cpu_backends, &value, sizeof(int64_t));
	}

	// net
	Decode obj_net[] =
	{
		{ DECODE_INT,    "connections",       &self->connections  },
		{ DECODE_INT,    "sent_bytes",        &self->sent         },
		{ DECODE_INT,    "recv_bytes",        &self->recv         },
		{ 0,              NULL,               NULL                },
	};
	decode_obj(obj_net, "net", &pos_net);

	// wal
	Decode obj_wal[] =
	{
		{ DECODE_INT,    "checkpoint",        &self->checkpoint    },
		{ DECODE_INT,    "lsn",               &self->lsn           },
		{ DECODE_INT,    "files",             &self->wals          },
		{ DECODE_INT,    "writes",            &self->transactions  },
		{ DECODE_INT,    "writes_bytes",      &self->wal_write,    },
		{ DECODE_INT,    "ops",               &self->ops           },
		{ 0,              NULL,               NULL                 },
	};
	decode_obj(obj_wal, "wal", &pos_wal);
}

void
top_init(Top* self, Remote* remote)
{
	memset(self, 0, sizeof(*self));
	self->remote = remote;
	json_init(&self->json);
}

void
top_free(Top* self)
{
	json_free(&self->json);
}

static inline void
top_request(Client* client, Str* content)
{
	// request
	auto request = &client->request;
	auto reply   = &client->reply;
	auto token   = remote_get(client->remote, REMOTE_TOKEN);

	// request
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Type", "text/plain");
	http_write(request, "Content-Length", "%d", str_size(content));
	http_write_end(request);
	tcp_write_pair_str(&client->tcp, &request->raw, content);

	// reply
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &client->readahead, &reply->content);

	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
	{
		auto code = &reply->options[HTTP_CODE];
		auto msg  = &reply->options[HTTP_MSG];
		error("%.*s %.*s\n", str_size(code), str_of(code),
		      str_size(msg), str_of(msg));
	}
}

static inline void
top_update(Top* self, Client* client)
{
	// execute show metrics
	Str req;
	str_set_cstr(&req, "show metrics");
	top_request(client, &req);

	Str data;
	buf_str(&client->reply.content, &data);
	json_reset(&self->json);
	json_parse(&self->json, &data, NULL);

	// allocate and read stats from the response
	auto stats = top_stats_allocate();
	errdefer(top_stats_free, stats);

	uint8_t* pos = self->json.buf_data.start;
	json_read_array(&pos);
	top_stats_read(stats, &pos);

	// push new stats
	switch (self->stats_count) {
	case 2:
		top_stats_free(self->stats[1]);
		self->stats[1] = self->stats[0];
		self->stats[0] = stats;
		break;
	case 1:
		self->stats[1] = self->stats[0];
		self->stats[0] = stats;
		self->stats_count++;
		break;
	case 0:
		self->stats[0] = stats;
		self->stats_count++;
		break;
	}
}

static inline Buf*
top_draw_cpu(int cpu_count, uint64_t cpu_diff, int count, Buf* abuf, Buf* bbuf)
{
	auto buf = buf_create();
	buf_write(buf, "[", 1);
	auto a = buf_u64(abuf);
	auto b = buf_u64(bbuf);
	if (buf_size(abuf) != buf_size(bbuf))
	{
		buf_write(buf, "]", 1);
		return buf;
	}
	for (int i = 0; i < count; i++)
	{
		auto usage = cpu_count * (double)(a[i] - b[i]) * 100 / (double)cpu_diff;
		if (i > 0)
			buf_write(buf, ", ", 2);
		buf_printf(buf, "%.1f", usage);
	}
	buf_write(buf, "]", 1);
	return buf;
}

#define PAD "        "

static inline void
top_draw(Top* self)
{
	auto stats = self->stats[0];
	auto prev  = self->stats[1];

	// move cursor
	write(STDOUT_FILENO, "\033[;H", 4);

	// calculate diffs between the last two stats
	double time_diff = (stats->time_us - prev->time_us) / 1000.0 / 1000.0;
	auto sent_diff   = stats->sent - prev->sent;
	auto sent        = ((double)sent_diff / 1024 / 1024) / time_diff;
	auto recv_diff   = stats->recv - prev->recv;
	auto recv        = ((double)recv_diff / 1024 / 1024) / time_diff;
	auto write_diff  = stats->wal_write - prev->wal_write;
	auto write       = ((double)write_diff / 1024 / 1024) / time_diff;
	auto trx_diff    = stats->transactions - prev->transactions;
	auto trx         = (double)trx_diff / time_diff;
	auto ops_diff    = stats->ops - prev->ops;
	auto ops         = (double)ops_diff / time_diff / 1000000.0;
	auto cpu_diff    = stats->cpu - prev->cpu;
	auto cpu_fe      = top_draw_cpu(stats->cpu_count, cpu_diff, stats->frontends,
	                                &stats->cpu_frontends,
	                                &prev->cpu_frontends);
	auto cpu_be      = top_draw_cpu(stats->cpu_count, cpu_diff, stats->backends,
	                                &stats->cpu_backends,
	                                &prev->cpu_backends);
	defer_buf(cpu_fe);
	defer_buf(cpu_be);

	info("uuid:              %.*s" PAD,
	     str_size(&stats->uuid), str_of(&stats->uuid));
	info("version:           %.*s" PAD,
	     str_size(&stats->version), str_of(&stats->version));
	info("frontends:         %-2" PRIi64 " %.*s" PAD PAD,
	     stats->frontends,
	     buf_size(cpu_fe), cpu_fe->start);
	info("backends:          %-2" PRIi64 " %.*s" PAD PAD,
	     stats->backends,
	     buf_size(cpu_be), cpu_be->start);
	info("memory:            %d MiB virt / %d Mib res" PAD,
	     (int)(stats->mem_virt / 1024 / 1024),
	     (int)(stats->mem_resident / 1024 / 1024));
	info("connections:       %" PRIi64 PAD, stats->connections);
	info("sent:              %.3f MiB/sec" PAD, sent);
	info("recv:              %.3f MiB/sec" PAD, recv);
	info("write:             %.3f MiB/sec" PAD, write);
	info("transactions:      %d n/sec" PAD, (int)trx);
	info("ops:               %.6f millions/sec" PAD, ops);
	info("wals:              %" PRIi64 PAD, stats->wals);
	info("checkpoint:        %" PRIi64 PAD, stats->checkpoint);
	info("lsn:               %" PRIi64 PAD, stats->lsn);
	info("schemas:           %" PRIi64 PAD, stats->schemas);
	info("tables:            %" PRIi64 PAD, stats->tables);
	info("secondary indexes: %" PRIi64 PAD, stats->secondary_indexes);
	info("");
}

static void
top_main(Top* self, Client* client)
{
	top_update(self, client);
	coroutine_sleep(500);

	// clean screen
	write(STDOUT_FILENO, "\x1b[H\x1b[2J", 7);

	for (;;)
	{
		// request new stats
		top_update(self, client);

		// update screen with diff
		top_draw(self);

		// 1sec
		coroutine_sleep(1000);
	}
}

void
top_run(Top* self)
{
	// create client and connect
	Client* client = NULL;
	error_catch
	(
		client = client_create();
		client_set_remote(client, self->remote);
		client_connect(client);

		top_main(self, client);
	);

	if (client)
	{
		client_close(client);
		client_free(client);
	}
}
