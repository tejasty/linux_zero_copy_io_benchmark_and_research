#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <liburing.h>

#define PORT 8080
#define BUFFER_SIZE 8192
#define QUEUE_DEPTH 256
#define CHUNK_SIZE (128 * 1024) // 128KB chunk for pipe transfers

int file_fd_4k, file_fd_50m;
size_t size_4k, size_50m;

// 1. Traditional Path: read() into user space buffer, then write() to socket
void handle_traditional(int client_fd, int file_fd, size_t file_size) {
    char header[256];
    int header_len = snprintf(header, sizeof(header),
             "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
             file_size);
    if (write(client_fd, header, header_len) <= 0) return;

    char buffer[BUFFER_SIZE];
    ssize_t bytes_read;
    off_t offset = 0;

    while (offset < file_size) {
        bytes_read = pread(file_fd, buffer, sizeof(buffer), offset);
        if (bytes_read <= 0) break;

        ssize_t total_written = 0;
        while (total_written < bytes_read) {
            ssize_t w = write(client_fd, buffer + total_written, bytes_read - total_written);
            if (w <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) continue;
                return; // Client disconnected or socket error
            }
            total_written += w;
        }
        offset += bytes_read;
    }
}

// 2. POSIX Zero-Copy Path: sendfile() transfers page cache directly to socket
void handle_zerocopy(int client_fd, int file_fd, size_t file_size) {
    char header[256];
    int header_len = snprintf(header, sizeof(header),
             "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
             file_size);
    if (write(client_fd, header, header_len) <= 0) return;

    off_t offset = 0;
    int retries = 0;
    while (offset < file_size) {
        ssize_t sent = sendfile(client_fd, file_fd, &offset, file_size - offset);
        if (sent <= 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                usleep(50); // Small backoff to allow socket queue to drain
                if (++retries > 1000) break; // Prevent infinite loop on dropped client
                continue;
            }
            break; // Socket error or client disconnect
        }
        retries = 0;
    }
}

// 3. Asynchronous Ring Zero-Copy Path: io_uring with non-blocking splice
void handle_io_uring(struct io_uring *ring, int client_fd, int file_fd, size_t file_size) {
    char header[256];
    int header_len = snprintf(header, sizeof(header),
             "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
             file_size);
    
    if (write(client_fd, header, header_len) <= 0) return;

    // FIX 1: splice requires a pipe to transfer data between a file and a socket.
    int pipefd[2];
    if (pipe(pipefd) < 0) {
        perror("pipe failed");
        return;
    }

    off_t offset = 0;
    while (offset < file_size) {
        // Determine how much to send in this chunk (max 128KB to fit in the pipe)
        size_t remaining = file_size - offset;
        size_t chunk = (remaining > CHUNK_SIZE) ? CHUNK_SIZE : remaining;

        // Get a Submission Queue Entry (SQE)
        struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            break; // Ring is full, wait for next iteration
        }

        // Prepare the async splice: File -> Pipe
        io_uring_prep_splice(sqe, file_fd, offset, pipefd[1], -1, chunk, 0);
        
        // Submit the SQE to the kernel
        io_uring_submit(ring);

        // FIX 2: Wait for the Completion Queue Event (CQE) 
        // We MUST wait for the kernel to finish before moving to the next step.
        struct io_uring_cqe *cqe;
        io_uring_wait_cqe(ring, &cqe);
        int result = cqe->res;
        io_uring_cqe_seen(ring, cqe); // Mark as seen so it can be reused

        if (result <= 0) {
            // Error occurred (e.g., client disconnected)
            break;
        }

        // Transfer from Pipe -> Socket (Synchronous for simplicity in this research prototype)
        int pipe_remaining = result;
        while (pipe_remaining > 0) {
            ssize_t sent = splice(pipefd[0], NULL, client_fd, NULL, pipe_remaining, 0);
            if (sent <= 0) {
                break; // Socket error or client disconnect
            }
            pipe_remaining -= sent;
        }

        offset += result; // Advance the file offset
    }

    // Clean up the pipe file descriptors to prevent memory leaks
    close(pipefd[0]);
    close(pipefd[1]);
}

int main() {
    int server_fd, client_fd;
    struct sockaddr_in address;
    socklen_t addrlen = sizeof(address);
    struct stat st_4k, st_50m;

    file_fd_4k = open("data/payload_4k.bin", O_RDONLY);
    file_fd_50m = open("data/payload_50m.bin", O_RDONLY);

    if (file_fd_4k < 0 || file_fd_50m < 0) {
        perror("Failed to open payload files. Make sure data/ folder exists.");
        exit(EXIT_FAILURE);
    }

    fstat(file_fd_4k, &st_4k); size_4k = st_4k.st_size;
    fstat(file_fd_50m, &st_50m); size_50m = st_50m.st_size;

    struct io_uring ring;
    if (io_uring_queue_init(QUEUE_DEPTH, &ring, 0) < 0) {
        perror("Failed to initialize io_uring ring");
        exit(EXIT_FAILURE);
    }

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    listen(server_fd, 2048);
    printf("Server running on port %d...\n", PORT);

    while (1) {
        client_fd = accept(server_fd, (struct sockaddr *)&address, &addrlen);
        if (client_fd < 0) continue;

        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        char request[1024] = {0};
        ssize_t r = read(client_fd, request, sizeof(request) - 1);
        
        if (r > 0) {
            if (strstr(request, "GET /traditional/4k")) {
                handle_traditional(client_fd, file_fd_4k, size_4k);
            } else if (strstr(request, "GET /traditional/50m")) {
                handle_traditional(client_fd, file_fd_50m, size_50m);
            } else if (strstr(request, "GET /zerocopy/4k")) {
                handle_zerocopy(client_fd, file_fd_4k, size_4k);
            } else if (strstr(request, "GET /zerocopy/50m")) {
                handle_zerocopy(client_fd, file_fd_50m, size_50m);
            } else if (strstr(request, "GET /iouring/4k")) {
                handle_io_uring(&ring, client_fd, file_fd_4k, size_4k);
            } else if (strstr(request, "GET /iouring/50m")) {
                handle_io_uring(&ring, client_fd, file_fd_50m, size_50m);
            }
        }

        close(client_fd);
    }

    io_uring_queue_exit(&ring);
    close(file_fd_4k);
    close(file_fd_50m);
    close(server_fd);
    return 0;
}