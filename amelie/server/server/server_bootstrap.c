
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
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>

#if 0
void
server_bootstrap(void)
{
	// set default server listen based on currently
	// active network interfaces
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// []
	encode_array(&buf);

	// get a list of network interfaces
	struct ifaddrs *ifap = NULL;
	auto rc = getifaddrs(&ifap);
	if (rc == -1)
		error_system();
	defer(freeifaddrs, ifap);

	for (auto ref = ifap; ref; ref = ref->ifa_next)
	{
		if (ref->ifa_addr == NULL)
			continue;

		socklen_t socklen;
		auto family = ref->ifa_addr->sa_family;
		switch (family) {
		case AF_INET:
			socklen = sizeof(struct sockaddr_in);
			break;
		case AF_INET6:
			socklen = sizeof(struct sockaddr_in6);
			break;
		default:
			continue;
		}

		// get interface ip address
		char host[NI_MAXHOST];
		rc = getnameinfo(ref->ifa_addr, socklen, host, NI_MAXHOST,
		                 NULL, 0, NI_NUMERICHOST);
		if (rc != 0)
		   error("getnameinfo(): %s", gai_strerror(rc));

		// tls and auto are disabled by default

		// {}
		encode_obj(&buf);
		// host
		encode_raw(&buf, "host", 4);
		encode_cstr(&buf, host);
		// port
		encode_raw(&buf, "port", 4);
		encode_integer(&buf, 3485);
		encode_obj_end(&buf);

#if 0
		if (ref->ifa_flags & IFF_LOOPBACK)
		{
			encode_obj(&buf);
			// host
			encode_raw(&buf, "host", 4);
			encode_cstr(&buf, host);
			// port
			encode_raw(&buf, "port", 4);
			encode_integer(&buf, 3485);

			encode_obj_end(&buf);
		} else
		{
			encode_obj(&buf);
			// host
			encode_raw(&buf, "host", 4);
			encode_cstr(&buf, host);
			// port
			encode_raw(&buf, "port", 4);
			encode_integer(&buf, 3485);
			encode_obj_end(&buf);
		}
#endif
	}

	encode_array_end(&buf);

	opt_json_set_buf(&config()->listen, &buf);
}
#endif

void
server_bootstrap(void)
{
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);

	// tls and auto are disabled by default

	// []
	encode_array(&buf);

	// {}
	encode_obj(&buf);
	// host
	encode_raw(&buf, "host", 4);
	encode_cstr(&buf, "*");
	// port
	encode_raw(&buf, "port", 4);
	encode_integer(&buf, 3485);
	encode_obj_end(&buf);

	encode_array_end(&buf);

	opt_json_set_buf(&config()->listen, &buf);
}
