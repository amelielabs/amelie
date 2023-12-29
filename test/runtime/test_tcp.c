
//
// indigo
//
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo.h>
#include <indigo_test.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

enum
{
	STOP,
	DATA
};

static void
test_tcp_server_client(void* arg)
{
	// pong
	Tcp* client = arg;
	for (;;)
	{
		auto buf = tcp_recv(client);
		test(buf);
		auto msg = msg_of(buf);
		if (msg->id == STOP)
		{
			buf_free(buf);
			break;
		}
		tcp_send(client, buf);
	}
	tcp_close(client);
	tcp_free(client);
	free(client);
}

static void
test_tcp_server(void *arg)
{
	Listen listen;
	listen_init(&listen);

	struct sockaddr_in sa;
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = inet_addr("127.0.0.1");
	sa.sin_port = htons(7778);
	listen_start(&listen, 4096, (struct sockaddr*)&sa);

	for (;;)
	{
		int fd = 0;
		Exception e;
		if (try(&e))
			fd = listen_accept(&listen);
		if (catch(&e))
			break;

		Tcp* client = mn_malloc(sizeof(Tcp));
		tcp_init(client);
		tcp_set_fd(client, fd);
		tcp_connect_fd(client);
		tcp_attach(client);
		test(tcp_connected(client));

		coroutine_create(test_tcp_server_client, client);
	}

	listen_stop(&listen);
}

static int client_recv = 0;

static void
test_tcp_client(void *arg)
{
	Tcp client;
	tcp_init(&client);

	struct sockaddr_in sa;
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = inet_addr("127.0.0.1");
	sa.sin_port = htons(7778);

	tcp_connect(&client, (struct sockaddr*)&sa);
	test(tcp_connected(&client));

	int total_send = 0;
	int total_recv = 0;

	// send
	int i = 0;
	while (i < 100)
	{
		auto buf = msg_create(DATA);
		buf_write(buf, "hello", 5);

		int random_size = rand() % 5000;
		buf_reserve(buf, random_size);
		memset(buf->position, 'x', random_size);
		buf_advance(buf, random_size);
		msg_end(buf);

		total_send += msg_of(buf)->size;
		tcp_send(&client, buf);
		i++;
	}

	// recv 
	i = 0;
	while (i < 100)
	{
		auto buf = tcp_recv(&client);
		test(buf);
		auto msg = msg_of(buf);
		test(msg->id == DATA);
		total_recv += msg->size;
		client_recv++;
		test(! memcmp(msg->data, "hello", 5));
		buf_free(buf);
		i++;
	}

	test(total_send == total_recv);

	// disconnect
	tcp_send(&client, msg_create(STOP));

	tcp_close(&client);
	tcp_free(&client);
}

void
test_tcp_10(void *arg)
{
	uint64_t server;
	server = coroutine_create(test_tcp_server, NULL);
	test( server != 0 );

	coroutine_sleep(0);

	uint64_t client[10];
	int i = 0;
	while (i < 10)
	{
		client[i] = coroutine_create(test_tcp_client, NULL);
		i++;
	}

	i = 0;
	while (i < 10)
	{
		coroutine_wait(client[i]);
		i++;
	}

	coroutine_sleep(0);

	test(client_recv == 1000);
	coroutine_kill(server);
}

void
test_tcp_100(void *arg)
{
	client_recv = 0;

	uint64_t server;
	server = coroutine_create(test_tcp_server, NULL);
	test( server != 0 );

	coroutine_sleep(0);

	uint64_t client[100];
	int i = 0;
	while (i < 100)
	{
		client[i] = coroutine_create(test_tcp_client, NULL);
		i++;
	}

	i = 0;
	while (i < 100)
	{
		coroutine_wait(client[i]);
		i++;
	}

	coroutine_sleep(0);

	test(client_recv == 10000);
	coroutine_kill(server);
}
