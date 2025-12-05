/*
 * MooseFS Community Edition - HA Status Tool
 * 
 * Command-line tool to monitor and manage the HA cluster
 * 
 * Usage:
 *   mfsha-status              - Show cluster status
 *   mfsha-status -v           - Verbose output
 *   mfsha-stepdown            - Step down from leadership
 *   mfsha-resync              - Request full resynchronization
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#include "sockets.h"
#include "datapack.h"
#include "strerr.h"
#include "MFSCommunication.h"

#define DEFAULT_MASTER_HOST "mfsmaster"
#define DEFAULT_MASTER_PORT "9421"

/* HA Protocol message types */
#define HA_CLI_GET_STATUS       0x0100
#define HA_CLI_STATUS_RESPONSE  0x0101
#define HA_CLI_STEP_DOWN        0x0102
#define HA_CLI_STEP_DOWN_ACK    0x0103
#define HA_CLI_REQUEST_RESYNC   0x0104
#define HA_CLI_RESYNC_ACK       0x0105

/* State names */
static const char* state_names[] = {
    "INIT",
    "FOLLOWER",
    "CANDIDATE",
    "LEADER",
    "OBSERVER",
    "SHUTDOWN"
};

static const char* state_colors[] = {
    "\033[0;37m",   /* INIT - gray */
    "\033[0;33m",   /* FOLLOWER - yellow */
    "\033[0;35m",   /* CANDIDATE - magenta */
    "\033[0;32m",   /* LEADER - green */
    "\033[0;36m",   /* OBSERVER - cyan */
    "\033[0;31m"    /* SHUTDOWN - red */
};

#define COLOR_RESET "\033[0m"
#define COLOR_BOLD  "\033[1m"

/* Options */
static int verbose = 0;
static int use_color = 1;
static char *master_host = NULL;
static char *master_port = NULL;

static void usage(const char *progname) {
    fprintf(stderr, 
        "MooseFS HA Status Tool\n"
        "\n"
        "Usage:\n"
        "  %s [options]                - Show cluster status\n"
        "  %s stepdown [options]       - Step down from leadership\n"
        "  %s resync [options]         - Request full resync\n"
        "\n"
        "Options:\n"
        "  -H <host>   Master hostname (default: %s)\n"
        "  -P <port>   Master port (default: %s)\n"
        "  -v          Verbose output\n"
        "  -n          No color output\n"
        "  -h          Show this help\n"
        "\n",
        progname, progname, progname,
        DEFAULT_MASTER_HOST, DEFAULT_MASTER_PORT);
}

static int connect_to_master(void) {
    int sock;
    uint32_t ip;
    uint16_t port;
    
    if (tcpresolve(master_host, master_port, &ip, &port, 0) < 0) {
        fprintf(stderr, "Error: Cannot resolve %s:%s\n", master_host, master_port);
        return -1;
    }
    
    sock = tcpsocket();
    if (sock < 0) {
        fprintf(stderr, "Error: Cannot create socket: %s\n", strerror(errno));
        return -1;
    }
    
    if (tcpnumconnect(sock, ip, port) < 0) {
        fprintf(stderr, "Error: Cannot connect to %s:%s: %s\n", 
                master_host, master_port, strerror(errno));
        tcpclose(sock);
        return -1;
    }
    
    return sock;
}

static void print_state(uint8_t state) {
    const char *name = (state < 6) ? state_names[state] : "UNKNOWN";
    if (use_color && state < 6) {
        printf("%s%s%s%s", COLOR_BOLD, state_colors[state], name, COLOR_RESET);
    } else {
        printf("%s", name);
    }
}

static void print_ip(uint32_t ip) {
    printf("%u.%u.%u.%u",
           (ip >> 24) & 0xFF,
           (ip >> 16) & 0xFF,
           (ip >> 8) & 0xFF,
           ip & 0xFF);
}

static int cmd_status(void) {
    int sock;
    uint8_t buffer[4096];
    uint8_t *ptr;
    ssize_t len;
    
    /* Header for request */
    uint8_t request[8];
    ptr = request;
    put32bit(&ptr, HA_CLI_GET_STATUS);
    put32bit(&ptr, 0);  /* No payload */
    
    sock = connect_to_master();
    if (sock < 0) {
        return 1;
    }
    
    /* Send request */
    if (write(sock, request, 8) != 8) {
        fprintf(stderr, "Error: Failed to send request\n");
        tcpclose(sock);
        return 1;
    }
    
    /* Read response */
    len = read(sock, buffer, sizeof(buffer));
    if (len < 8) {
        fprintf(stderr, "Error: Invalid response from master\n");
        tcpclose(sock);
        return 1;
    }
    
    tcpclose(sock);
    
    /* Parse response */
    ptr = buffer;
    uint32_t msg_type = get32bit((const uint8_t**)&ptr);
    uint32_t msg_len = get32bit((const uint8_t**)&ptr);
    
    if (msg_type != HA_CLI_STATUS_RESPONSE) {
        fprintf(stderr, "Error: Unexpected response type\n");
        return 1;
    }
    
    if (len < (ssize_t)(8 + msg_len)) {
        fprintf(stderr, "Error: Incomplete response\n");
        return 1;
    }
    
    /* Parse HA status */
    uint8_t enabled = get8bit((const uint8_t**)&ptr);
    
    printf("\n");
    if (use_color) printf("%s", COLOR_BOLD);
    printf("MooseFS HA Cluster Status\n");
    printf("=========================\n");
    if (use_color) printf("%s", COLOR_RESET);
    printf("\n");
    
    if (!enabled) {
        printf("HA Status: ");
        if (use_color) printf("\033[0;31m");
        printf("DISABLED");
        if (use_color) printf("%s", COLOR_RESET);
        printf("\n\n");
        printf("To enable HA, configure HA_ENABLED=1 in mfsha.cfg\n");
        return 0;
    }
    
    uint8_t state = get8bit((const uint8_t**)&ptr);
    uint64_t term = get64bit((const uint8_t**)&ptr);
    uint32_t leader_id = get32bit((const uint8_t**)&ptr);
    uint32_t leader_ip = get32bit((const uint8_t**)&ptr);
    uint64_t commit_index = get64bit((const uint8_t**)&ptr);
    uint64_t last_applied = get64bit((const uint8_t**)&ptr);
    uint32_t peer_count = get32bit((const uint8_t**)&ptr);
    uint32_t connected_peers = get32bit((const uint8_t**)&ptr);
    
    printf("State:           ");
    print_state(state);
    printf("\n");
    
    printf("Term:            %lu\n", term);
    
    printf("Leader:          ");
    if (state == 3) {  /* LEADER */
        printf("(this node)\n");
    } else if (leader_ip != 0) {
        print_ip(leader_ip);
        printf(" (id: %u)\n", leader_id);
    } else {
        printf("unknown\n");
    }
    
    printf("Commit Index:    %lu\n", commit_index);
    
    if (verbose) {
        printf("Last Applied:    %lu\n", last_applied);
    }
    
    printf("Peers:           %u/%u connected\n", connected_peers, peer_count);
    
    /* Print peer details if available and verbose */
    if (verbose && peer_count > 0) {
        printf("\n");
        printf("Peer Details:\n");
        printf("-------------\n");
        
        /* Would need more data in the response for this */
        printf("(detailed peer info requires -v flag and extended protocol)\n");
    }
    
    printf("\n");
    
    /* Health assessment */
    printf("Cluster Health:  ");
    if (state == 3 || state == 1) {  /* LEADER or FOLLOWER */
        if (connected_peers + 1 > peer_count / 2) {  /* Have quorum */
            if (use_color) printf("\033[0;32m");
            printf("HEALTHY");
        } else {
            if (use_color) printf("\033[0;33m");
            printf("DEGRADED (lost quorum)");
        }
    } else if (state == 2) {  /* CANDIDATE */
        if (use_color) printf("\033[0;33m");
        printf("ELECTION IN PROGRESS");
    } else {
        if (use_color) printf("\033[0;31m");
        printf("UNHEALTHY");
    }
    if (use_color) printf("%s", COLOR_RESET);
    printf("\n\n");
    
    return 0;
}

static int cmd_stepdown(void) {
    int sock;
    uint8_t buffer[256];
    uint8_t *ptr;
    ssize_t len;
    
    printf("Requesting leadership step-down...\n");
    
    /* Header for request */
    uint8_t request[8];
    ptr = request;
    put32bit(&ptr, HA_CLI_STEP_DOWN);
    put32bit(&ptr, 0);
    
    sock = connect_to_master();
    if (sock < 0) {
        return 1;
    }
    
    if (write(sock, request, 8) != 8) {
        fprintf(stderr, "Error: Failed to send request\n");
        tcpclose(sock);
        return 1;
    }
    
    len = read(sock, buffer, sizeof(buffer));
    tcpclose(sock);
    
    if (len < 8) {
        fprintf(stderr, "Error: Invalid response\n");
        return 1;
    }
    
    ptr = buffer;
    uint32_t msg_type = get32bit((const uint8_t**)&ptr);
    
    if (msg_type == HA_CLI_STEP_DOWN_ACK) {
        uint8_t status = get8bit((const uint8_t**)&ptr);
        if (status == 0) {
            printf("Successfully stepped down from leadership.\n");
            printf("A new leader election will begin shortly.\n");
            return 0;
        } else if (status == 1) {
            printf("This node is not the current leader.\n");
            return 0;
        } else {
            fprintf(stderr, "Error: Step-down failed (status=%u)\n", status);
            return 1;
        }
    }
    
    fprintf(stderr, "Error: Unexpected response\n");
    return 1;
}

static int cmd_resync(void) {
    int sock;
    uint8_t buffer[256];
    uint8_t *ptr;
    ssize_t len;
    
    printf("Requesting full metadata resynchronization...\n");
    printf("Warning: This may take some time for large metadata sets.\n\n");
    
    uint8_t request[8];
    ptr = request;
    put32bit(&ptr, HA_CLI_REQUEST_RESYNC);
    put32bit(&ptr, 0);
    
    sock = connect_to_master();
    if (sock < 0) {
        return 1;
    }
    
    if (write(sock, request, 8) != 8) {
        fprintf(stderr, "Error: Failed to send request\n");
        tcpclose(sock);
        return 1;
    }
    
    len = read(sock, buffer, sizeof(buffer));
    tcpclose(sock);
    
    if (len < 8) {
        fprintf(stderr, "Error: Invalid response\n");
        return 1;
    }
    
    ptr = buffer;
    uint32_t msg_type = get32bit((const uint8_t**)&ptr);
    
    if (msg_type == HA_CLI_RESYNC_ACK) {
        uint8_t status = get8bit((const uint8_t**)&ptr);
        if (status == 0) {
            printf("Resynchronization request accepted.\n");
            printf("Use 'mfsha-status' to monitor progress.\n");
            return 0;
        } else if (status == 1) {
            printf("Cannot resync: this node is the leader.\n");
            return 0;
        } else if (status == 2) {
            printf("Resync already in progress.\n");
            return 0;
        } else {
            fprintf(stderr, "Error: Resync request failed (status=%u)\n", status);
            return 1;
        }
    }
    
    fprintf(stderr, "Error: Unexpected response\n");
    return 1;
}

int main(int argc, char *argv[]) {
    int opt;
    const char *command = "status";
    
    /* Initialize defaults */
    master_host = strdup(DEFAULT_MASTER_HOST);
    master_port = strdup(DEFAULT_MASTER_PORT);
    
    /* Check if stdout is a terminal for color support */
    use_color = isatty(STDOUT_FILENO);
    
    /* Parse command (if provided) */
    if (argc > 1 && argv[1][0] != '-') {
        command = argv[1];
        optind = 2;
    }
    
    /* Parse options */
    while ((opt = getopt(argc, argv, "H:P:vnh")) != -1) {
        switch (opt) {
            case 'H':
                free(master_host);
                master_host = strdup(optarg);
                break;
            case 'P':
                free(master_port);
                master_port = strdup(optarg);
                break;
            case 'v':
                verbose = 1;
                break;
            case 'n':
                use_color = 0;
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }
    
    /* Initialize socket library */
    strerr_init();
    
    /* Execute command */
    int result;
    if (strcmp(command, "status") == 0) {
        result = cmd_status();
    } else if (strcmp(command, "stepdown") == 0) {
        result = cmd_stepdown();
    } else if (strcmp(command, "resync") == 0) {
        result = cmd_resync();
    } else {
        fprintf(stderr, "Unknown command: %s\n", command);
        usage(argv[0]);
        result = 1;
    }
    
    free(master_host);
    free(master_port);
    
    return result;
}
