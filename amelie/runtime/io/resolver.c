
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>

void
resolve(const char*       addr,
        int               port,
        struct addrinfo **result)
{
	char service[16];
	snprintf(service, sizeof(service), "%d", port);

	struct addrinfo  hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family   = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags    = AI_PASSIVE;
	hints.ai_protocol = IPPROTO_TCP;

	auto rc = socket_getaddrinfo(addr, service, &hints, result);
	if (rc == -1 || *result == NULL)
		error("failed to resolve %s:%d", addr, port);
}
