// Linux process-test faults: reject thread creation or hold a log prefetch open.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int open_with_delay(const char *path, int flags, mode_t mode, const char *symbol) {
    int (*real_open)(const char *, int, ...) = dlsym(RTLD_NEXT, symbol);
    const char *blocked = getenv("DAR_TEST_BLOCKED_OPEN");
    const char *latest = getenv("DAR_TEST_LATEST_LOG");
    const char *started = getenv("DAR_TEST_OPEN_STARTED");
    if (blocked && latest && started && (flags & O_ACCMODE) == O_RDONLY) {
        if (strcmp(path, blocked) == 0) {
            int marker = real_open(started, O_WRONLY | O_CREAT, 0600);
            if (marker >= 0) close(marker);
            sleep(30); // Longer than the process test's 15-second deadline.
        } else if (strcmp(path, latest) == 0) {
            // Ensure the unused prefetch is in flight before returning metadata.
            for (int i = 0; i < 30000 && access(started, F_OK) != 0; ++i) {
                usleep(1000);
            }
        }
    }
    return real_open(path, flags, mode);
}

int open(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    return open_with_delay(path, flags, mode, "open");
}

int open64(const char *path, int flags, ...) {
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list args;
        va_start(args, flags);
        mode = va_arg(args, mode_t);
        va_end(args);
    }
    return open_with_delay(path, flags, mode, "open64");
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                   void *(*start)(void *), void *arg) {
    if (getenv("DAR_TEST_REJECT_THREADS")) return EAGAIN;
    int (*real_create)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *) =
        dlsym(RTLD_NEXT, "pthread_create");
    return real_create(thread, attr, start, arg);
}
