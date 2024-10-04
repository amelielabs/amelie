
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>

void
server_bootstrap(void)
{
	// set default server listen based on currently
	// active network interfaces
	Buf buf;
	buf_init(&buf);
	guard_buf(&buf);

	// []
	encode_array(&buf);

	// unixsocket
	encode_map(&buf);
	// path
	encode_raw(&buf, "path", 4);
	encode_raw(&buf, "amelie.sock", 11);
	// path_mode
	encode_raw(&buf, "path_mode", 9);
	encode_raw(&buf, "0700", 4);
	encode_map_end(&buf);

	// get a list of network interfaces
	struct ifaddrs *ifap = NULL;
	auto rc = getifaddrs(&ifap);
	if (rc == -1)
		error_system();
	guard(freeifaddrs, ifap);

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

		if (ref->ifa_flags & IFF_LOOPBACK)
		{
			encode_map(&buf);
			// host
			encode_raw(&buf, "host", 4);
			encode_cstr(&buf, host);
			// port
			encode_raw(&buf, "port", 4);
			encode_integer(&buf, 3485);
			encode_map_end(&buf);
		} else
		{
			encode_map(&buf);
			// host
			encode_raw(&buf, "host", 4);
			encode_cstr(&buf, host);
			// port
			encode_raw(&buf, "port", 4);
			encode_integer(&buf, 3485);
			encode_map_end(&buf);
		}
	}

	encode_array_end(&buf);

	var_data_set_buf(&config()->listen, &buf);
}
