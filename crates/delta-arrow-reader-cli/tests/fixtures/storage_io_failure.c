/* Inject storage errors only in the selected temporary-output files. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

static void mark_fault(void) {
    int marker = open(getenv("DAR_STORAGE_FAULT_MARKER"), O_WRONLY | O_CREAT, 0600);
    if (marker >= 0) close(marker);
}

static int fail_storage_write(int fd) {
    const char *error = getenv("DAR_STORAGE_FAULT_ERRNO");
    if (!error) return 0;
    char descriptor[64], path[PATH_MAX];
    snprintf(descriptor, sizeof(descriptor), "/proc/self/fd/%d", fd);
    ssize_t length = readlink(descriptor, path, sizeof(path) - 1);
    if (length < 0) return 0;
    path[length] = '\0';
    const char *pattern = getenv("DAR_STORAGE_FAULT_FILE");
    if (pattern) {
        if (!strstr(path, pattern)) return 0;
    } else if (!strstr(path, "/dar-query-") || !strstr(path, "/datafusion-")) return 0;
    mark_fault();
    errno = atoi(error);
    return 1;
}

ssize_t write(int fd, const void *buffer, size_t length) {
    if (fail_storage_write(fd)) return -1;
    ssize_t (*original)(int, const void *, size_t) = dlsym(RTLD_NEXT, "write");
    return original(fd, buffer, length);
}

ssize_t writev(int fd, const struct iovec *buffers, int count) {
    if (fail_storage_write(fd)) return -1;
    ssize_t (*original)(int, const struct iovec *, int) = dlsym(RTLD_NEXT, "writev");
    return original(fd, buffers, count);
}

static int deny_spill_removal(const char *path) {
    const char *name = strrchr(path, '/');
    name = name ? name + 1 : path;
    if (!getenv("DAR_STORAGE_FAULT_REMOVE") || strncmp(name, "dar-query-", 10)) return 0;
    mark_fault();
    const char *delay = getenv("DAR_STORAGE_FAULT_REMOVE_DELAY_MS");
    if (delay) usleep(atoi(delay) * 1000);
    const char *error = getenv("DAR_STORAGE_FAULT_REMOVE_ERRNO");
    errno = error ? atoi(error) : EACCES;
    return 1;
}

int unlinkat(int fd, const char *path, int flags) {
    if (deny_spill_removal(path)) return -1;
    int (*original)(int, const char *, int) = dlsym(RTLD_NEXT, "unlinkat");
    return original(fd, path, flags);
}

int rmdir(const char *path) {
    if (deny_spill_removal(path)) return -1;
    int (*original)(const char *) = dlsym(RTLD_NEXT, "rmdir");
    return original(path);
}
