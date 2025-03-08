
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

void
daemon_init(Daemon* self)
{
	self->cmd          = DAEMON_OTHER;
	self->daemon       = false;
	self->directory    = NULL;
	self->directory_fd = -1;
	self->argc         = 0;
	self->argv         = NULL;
}

static int
daemon_prepare(Daemon* self, int argc, char** argv)
{
	self->argc = argc;
	self->argv = argv;

	if (argc >= 2 && !strcmp(argv[1], "start"))
	{
		self->cmd = DAEMON_START;
		if (argc < 3)
		{
			printf("error: start <path> expected\n");
			return -1;
		}

		// path
		self->directory = argv[2];

		// --daemon=true
		// --daemon
		for (auto i = 3; i < argc; i++) {
			if (!strcmp(argv[i], "--daemon") ||
				!strcmp(argv[i], "--daemon=true"))
			{
				self->daemon = true;
				break;
			}
		}
	} else
	if (argc >= 2 && !strcmp(argv[1], "stop"))
	{
		self->cmd = DAEMON_STOP;
		if (argc < 3)
		{
			printf("error: stop <path> expected\n");
			return -1;
		}

		// path
		self->directory = argv[2];
	} else
	{
		self->cmd = DAEMON_OTHER;
	}

	return 0;
}

static int
daemon_read_pidfile(Daemon* self)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/pid", self->directory);
	auto fd = vfs_open(path, O_RDONLY, 0644);
	if (fd == -1)
	{
		printf("error: pid file '%s' open error (%s)\n", path,
		       strerror(errno));
		return -1;
	}
	char pid_sz[32];
	auto pid_len = vfs_size_fd(fd);
	if (pid_len > (int64_t)sizeof(pid_sz))
	{
		printf("error: invalid pid file '%s'\n", path);
		return -1;
	}
	if (vfs_read(fd, pid_sz, pid_len) != pid_len)
	{
		printf("error: pid file '%s' read error (%s)\n", path,
		       strerror(errno));
		return -1;
	}
	close(fd);
	int64_t pid = -1;
	Str str;
	str_set(&str, pid_sz, pid_len);
	if (str_toint(&str, &pid) == -1)
	{
		printf("error: invalid pid file '%s'\n", path);
		return -1;
	}
	return pid;
}

static int
daemon_write_pidfile(Daemon* self, pid_t pid)
{
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/pid", self->directory);
	auto fd = vfs_open(path, O_TRUNC|O_CREAT|O_WRONLY, 0644);
	if (fd == -1)
	{
		printf("error: pid file '%s' create error (%s)\n", path,
		       strerror(errno));
		return -1;
	}

	char pid_sz[32];
	auto pid_len = snprintf(pid_sz, sizeof(pid_sz), "%d", pid);
	if (vfs_write(fd, pid_sz, pid_len) != pid_len)
	{
		printf("error: pid file '%s' write error (%s)\n", path,
		       strerror(errno));
		return -1;
	}

	close(fd);
	return 0;
}

static int
daemon_open(Daemon* self)
{
	// ensure directory exists
	struct stat st;
	auto rc = stat(self->directory, &st);
	if (rc == -1)
	{
		printf("error: directory '%s' open error (%s)\n", self->directory,
		       strerror(errno));
		return -1;
	}

	// open directory fd
	rc = vfs_open(self->directory, O_DIRECTORY|O_RDONLY, 0755);
	if (rc == -1)
	{
		printf("error: directory '%s' open error (%s)\n", self->directory,
		       strerror(errno));
		return -1;
	}
	self->directory_fd = rc;
	return 0;
}

static inline int
daemon_fork(Daemon* self)
{
	pid_t pid = fork();
	if (pid == -1)
	{
		printf("error: fork() failed (%s)\n", strerror(errno));
		return -1;
	}

	// parent
	if (pid > 0)
	{
		// create pid file by parent before exit
		daemon_write_pidfile(self, pid);
		_exit(EXIT_SUCCESS);
	}

	setsid();

	auto fd = open("/dev/null", O_RDWR);
	if (fd == -1)
	{
		printf("error: open() failed (%s)\n", strerror(errno));
		return -1;
	}

	dup2(fd, 0);
	dup2(fd, 1);
	dup2(fd, 2);
	if (fd > 2)
		close(fd);
	return 0;
}

static inline int
daemon_start(Daemon* self)
{
	// open directory
	auto rc = daemon_open(self);
	if (rc == -1)
		return -1;

	// try to take exclusive directory lock
	rc = vfs_flock_exclusive(self->directory_fd);
	if (rc == -1)
	{
		if (errno == EWOULDBLOCK)
			printf("error: instance already started\n");
		else
			printf("error: directory '%s' failed to take directory lock (%s)\n",
			       self->directory, strerror(errno));
		return -1;
	}

	// daemonize
	if (self->daemon)
		return daemon_fork(self);

	return daemon_write_pidfile(self, getpid());
}

static inline int
daemon_stop(Daemon* self)
{
	// open directory
	auto rc = daemon_open(self);
	if (rc == -1)
		return -1;

	// try to take exclusive directory lock
	rc = vfs_flock_exclusive(self->directory_fd);
	if (rc == 0)
	{
		// instance is not active
		return 0;
	}
	if (rc == -1 && errno != EWOULDBLOCK)
	{
		printf("error: directory '%s' failed to take directory lock (%s)\n",
		       self->directory, strerror(errno));
		return -1;
	}

	// read pidfile
	auto pid = daemon_read_pidfile(self);
	if (pid == -1)
		return -1;

	kill(pid, SIGINT);

	int status;
	waitpid(pid, &status, WNOHANG);
	return 0;
}

int
daemon_main(Daemon* self, int argc, char** argv)
{
	auto rc = daemon_prepare(self, argc, argv);
	if (rc == -1)
		return -1;
	switch (self->cmd) {
	case DAEMON_START:
		return daemon_start(self);
	case DAEMON_STOP:
		return daemon_stop(self);
	case DAEMON_OTHER:
		break;
	}
	return 0;
}

void
daemon_wait_for_signal(void)
{
	// wait signal for completion
	sigset_t mask;
	sigfillset(&mask);
	pthread_sigmask(SIG_BLOCK, &mask, NULL);
	sigemptyset(&mask);
	sigaddset(&mask, SIGINT);
	sigaddset(&mask, SIGTERM);
	int signo;
	sigwait(&mask, &signo);
}
