/*
 * MooseFS Community Edition - HA Integration Module
 *
 * Copyright (C) 2025 - Open Source Community Contribution
 *
 * Implementation of HA integration with MooseFS master.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <netinet/in.h>
#include <limits.h>

#include "ha_integration.h"
#include "hamanager.h"
#include "ha_sync.h"
#include "cfg.h"
#include "main.h"
#include "mfslog.h"
#include "metadata.h"
#include "changelog.h"
#include "matoclserv.h"
#include "clocks.h"
#include "datapack.h"
#include "restore.h"
#include "sockets.h"
#include "MFSCommunication.h"
#include "crc.h"

/* External MooseFS functions for changelog handling */
extern uint64_t meta_version(void);

/* ============================================================================
 * Static Variables
 * ============================================================================ */

static uint8_t ha_enabled = 0;
static pthread_mutex_t ha_int_mutex = PTHREAD_MUTEX_INITIALIZER;
static char status_buffer[1024];

/* Cached leader information */
static uint32_t cached_leader_ip = 0;
static uint16_t cached_leader_port = 0;

/* Synchronization state */
static uint8_t sync_in_progress = 0;
static uint8_t sync_needed = 0;
static uint64_t last_sync_request_time = 0;
static uint64_t changelog_buffer_count = 0;
static char *data_path_cache = NULL;

/* Changelog file for persistence (like metalogger) */
static FILE *changelog_fd = NULL;

/* Changelog buffer for when sync is in progress */
#define CHANGELOG_BUFFER_SIZE 1000
typedef struct {
    uint64_t version;
    uint8_t *data;
    uint32_t len;
} changelog_entry_t;

static changelog_entry_t changelog_buffer[CHANGELOG_BUFFER_SIZE];
static uint32_t changelog_buffer_head = 0;
static uint32_t changelog_buffer_tail = 0;

/* ============================================================================
 * Pre-metadata Sync (called BEFORE meta_init)
 * ============================================================================ */

/*
 * Check if metadata file exists and is valid
 */
static int metadata_file_exists(const char *data_path) {
    char path[PATH_MAX];
    struct stat st;
    int fd;
    char header[8];

    /* Check metadata.mfs */
    snprintf(path, sizeof(path), "%s/metadata.mfs", data_path);
    if (stat(path, &st) == 0 && st.st_size > 1000) {
        fd = open(path, O_RDONLY);
        if (fd >= 0) {
            if (read(fd, header, 8) == 8) {
                close(fd);
                /* Check for valid signature, not "MFSM NEW" */
                if (memcmp(header, "MFSM ", 5) == 0 &&
                    memcmp(header, "MFSM NEW", 8) != 0) {
                    return 1;
                }
            } else {
                close(fd);
            }
        }
    }

    /* Check metadata.mfs.back */
    snprintf(path, sizeof(path), "%s/metadata.mfs.back", data_path);
    if (stat(path, &st) == 0 && st.st_size > 1000) {
        fd = open(path, O_RDONLY);
        if (fd >= 0) {
            if (read(fd, header, 8) == 8) {
                close(fd);
                /* Check for valid signature, not "MFSM NEW" */
                if (memcmp(header, "MFSM ", 5) == 0 &&
                    memcmp(header, "MFSM NEW", 8) != 0) {
                    return 1;
                }
            } else {
                close(fd);
            }
        }
    }

    return 0;
}

/*
 * Verify metadata file integrity (like metalogger)
 * Check signature and EOF marker
 */
static int metadata_check(const char *name) {
    int fd;
    char chkbuff[16];
    char eofmark[16];

    fd = open(name, O_RDONLY);
    if (fd < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "HA: Can't open downloaded metadata");
        return -1;
    }

    if (read(fd, chkbuff, 8) != 8) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "HA: Can't read downloaded metadata");
        close(fd);
        return -1;
    }

    if (memcmp(chkbuff, "MFSM NEW", 8) == 0) {
        close(fd);
        return -1;  /* Empty metadata */
    }

    /* Check signature "MFSM x.y" */
    if (memcmp(chkbuff, "MFSM ", 5) == 0 && chkbuff[5] >= '1' && chkbuff[5] <= '9'
        && chkbuff[6] == '.' && chkbuff[7] >= '0' && chkbuff[7] <= '9') {
        uint8_t fver = ((chkbuff[5] - '0') << 4) + (chkbuff[7] - '0');
        if (fver < 0x17) {
            memset(eofmark, 0, 16);
        } else {
            memcpy(eofmark, "[MFS EOF MARKER]", 16);
        }
    } else {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "HA: Bad metadata file format");
        close(fd);
        return -1;
    }

    /* Check EOF marker */
    lseek(fd, -16, SEEK_END);
    if (read(fd, chkbuff, 16) != 16) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "HA: Can't read EOF marker");
        close(fd);
        return -1;
    }
    close(fd);

    if (memcmp(chkbuff, eofmark, 16) != 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "HA: Truncated metadata file!");
        return -1;
    }

    return 0;
}

/*
 * Send a complete HA message with header
 */
static int send_ha_message(int sock, uint16_t type, const uint8_t *payload, uint32_t len) {
    ha_msg_header_t header;

    header.magic = HA_MSG_MAGIC;
    header.type = type;
    header.version = HA_PROTOCOL_VER;
    header.sender_id = 0;  /* Will be filled by receiver */
    header.term = 0;       /* Not used for sync */
    header.length = len;
    header.crc32 = (len > 0 && payload != NULL) ? mycrc32(0, payload, len) : 0;

    if (tcptowrite(sock, &header, sizeof(header), 1000, 5000) != sizeof(header)) {
        return -1;
    }

    if (len > 0 && payload != NULL) {
        if (tcptowrite(sock, payload, len, 1000, 30000) != (ssize_t)len) {
            return -1;
        }
    }

    return 0;
}

/*
 * Receive a complete HA message
 */
static int recv_ha_message(int sock, uint16_t *type, uint8_t **payload, uint32_t *len, uint32_t timeout_ms) {
    ha_msg_header_t header;
    uint32_t calc_crc;

    if (tcptoread(sock, &header, sizeof(header), 1000, timeout_ms) != sizeof(header)) {
        return -1;
    }

    if (header.magic != HA_MSG_MAGIC) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, "HA: Bad magic in response");
        return -1;
    }

    *type = header.type;
    *len = header.length;

    if (header.length > 0) {
        *payload = malloc(header.length);
        if (*payload == NULL) {
            return -1;
        }

        if (tcptoread(sock, *payload, header.length, 1000, timeout_ms) != (ssize_t)header.length) {
            free(*payload);
            *payload = NULL;
            return -1;
        }

        calc_crc = mycrc32(0, *payload, header.length);
        if (calc_crc != header.crc32) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA: Message CRC mismatch (got 0x%08X, expected 0x%08X)", calc_crc, header.crc32);
            free(*payload);
            *payload = NULL;
            return -1;
        }
    } else {
        *payload = NULL;
    }

    return 0;
}

/*
 * Download metadata from a peer using chunked transfer with CRC
 * Like metalogger but synchronous for startup
 */
static int download_metadata_from_peer(uint32_t ip, uint16_t port, const char *data_path) {
    int sock;
    uint8_t request[16];
    uint8_t *wptr;
    uint8_t *payload = NULL;
    const uint8_t *rptr;
    uint16_t msg_type;
    uint32_t msg_len;
    char path[PATH_MAX];
    char temp_path[PATH_MAX];
    int fd = -1;
    uint64_t file_size, offset;
    uint64_t leader_version;
    uint32_t chunk_size, crc, calc_crc;
    uint64_t chunk_offset;
    int retry_count;
    int result = -1;

    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
            "HA Pre-sync: Connecting to peer %u.%u.%u.%u:%u",
            (ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF, port);

    /* Connect to peer */
    sock = tcpsocket();
    if (sock < 0) {
        return -1;
    }

    if (tcpnumconnect(sock, ip, port) < 0) {
        tcpclose(sock);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Pre-sync: Failed to connect to peer");
        return -1;
    }

    tcpnodelay(sock);

    /* Send sync request */
    wptr = request;
    put64bit(&wptr, 0);  /* Our version (0 = need full sync) */
    put64bit(&wptr, 0);  /* Checksum */

    if (send_ha_message(sock, HA_MSG_SYNC_REQUEST, request, 16) < 0) {
        tcpclose(sock);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Pre-sync: Failed to send sync request");
        return -1;
    }

    /* Receive SYNC_INFO with file size */
    if (recv_ha_message(sock, &msg_type, &payload, &msg_len, 30000) < 0) {
        tcpclose(sock);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Pre-sync: Failed to receive sync info");
        return -1;
    }

    if (msg_type != HA_MSG_SYNC_INFO || msg_len < 16) {
        free(payload);
        tcpclose(sock);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Pre-sync: Unexpected response type %u", msg_type);
        return -1;
    }

    rptr = payload;
    leader_version = get64bit(&rptr);
    file_size = get64bit(&rptr);
    free(payload);
    payload = NULL;

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Pre-sync: Leader version=%"PRIu64", file size=%"PRIu64,
            leader_version, file_size);

    /* Open temp file */
    snprintf(temp_path, sizeof(temp_path), "%s/metadata.mfs.ha_sync", data_path);
    snprintf(path, sizeof(path), "%s/metadata.mfs.back", data_path);

    fd = open(temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        tcpclose(sock);
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Pre-sync: Failed to create temp file: %s", strerror(errno));
        return -1;
    }

    /* Download chunks */
    offset = 0;
    retry_count = 0;

    while (offset < file_size) {
        chunk_size = HA_META_DL_BLOCK;
        if (file_size - offset < chunk_size) {
            chunk_size = file_size - offset;
        }

        /* Request chunk */
        wptr = request;
        put64bit(&wptr, offset);
        put32bit(&wptr, chunk_size);

        if (send_ha_message(sock, HA_MSG_SYNC_CHUNK_REQUEST, request, 12) < 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: Failed to request chunk at offset %"PRIu64, offset);
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        /* Receive chunk */
        if (recv_ha_message(sock, &msg_type, &payload, &msg_len, 60000) < 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: Failed to receive chunk");
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        if (msg_type != HA_MSG_SYNC_CHUNK_DATA || msg_len < 16) {
            free(payload);
            payload = NULL;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: Unexpected chunk response type %u", msg_type);
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        /* Parse chunk: offset(8) + size(4) + crc(4) + data */
        rptr = payload;
        chunk_offset = get64bit(&rptr);
        chunk_size = get32bit(&rptr);
        crc = get32bit(&rptr);

        if (msg_len != 16 + chunk_size) {
            free(payload);
            payload = NULL;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: Chunk size mismatch");
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        if (chunk_offset != offset) {
            free(payload);
            payload = NULL;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: Chunk offset mismatch (%"PRIu64" vs %"PRIu64")",
                    chunk_offset, offset);
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        /* Verify CRC */
        calc_crc = mycrc32(0, payload + 16, chunk_size);
        if (calc_crc != crc) {
            free(payload);
            payload = NULL;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: Chunk CRC mismatch (got 0x%08X, expected 0x%08X)",
                    calc_crc, crc);
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        /* Write chunk */
        if (pwrite(fd, payload + 16, chunk_size, offset) != (ssize_t)chunk_size) {
            free(payload);
            payload = NULL;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                    "HA Pre-sync: Failed to write chunk");
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        /* fsync after each chunk (like metalogger) */
        if (fsync(fd) < 0) {
            free(payload);
            payload = NULL;
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Pre-sync: fsync failed");
            if (++retry_count >= 5) {
                goto cleanup;
            }
            continue;
        }

        free(payload);
        payload = NULL;

        offset += chunk_size;
        retry_count = 0;  /* Reset retry counter on success */

        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                "HA Pre-sync: Downloaded %"PRIu64"/%"PRIu64" bytes (%.1f%%)",
                offset, file_size, (100.0 * offset) / file_size);
    }

    close(fd);
    fd = -1;
    tcpclose(sock);
    sock = -1;

    /* Verify metadata file */
    if (metadata_check(temp_path) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Pre-sync: Downloaded metadata failed verification");
        unlink(temp_path);
        return -1;
    }

    /* Rename to final location */
    if (rename(temp_path, path) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Pre-sync: Failed to rename metadata file");
        unlink(temp_path);
        return -1;
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Pre-sync: Successfully downloaded and verified metadata (%"PRIu64" bytes)",
            file_size);

    return 0;

cleanup:
    if (payload) free(payload);
    if (fd >= 0) close(fd);
    if (sock >= 0) tcpclose(sock);
    unlink(temp_path);
    return result;
}

/*
 * Parse peers from config string "ip1:port1,ip2:port2,..."
 */
static int parse_and_try_peers(const char *peers_str, const char *data_path) {
    char *peers_copy;
    char *peer, *saveptr;
    char *host, *port_str;
    uint32_t ip;
    uint16_t port;

    if (!peers_str || !peers_str[0]) {
        return -1;
    }

    peers_copy = strdup(peers_str);
    if (!peers_copy) {
        return -1;
    }

    peer = strtok_r(peers_copy, ",", &saveptr);
    while (peer) {
        /* Skip whitespace */
        while (*peer == ' ') peer++;

        host = peer;
        port_str = strchr(peer, ':');
        if (port_str) {
            *port_str = '\0';
            port_str++;
            port = atoi(port_str);
        } else {
            port = 9418;  /* Default HA port */
        }

        /* Resolve hostname */
        if (tcpresolve(host, NULL, &ip, NULL, 0) >= 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
                    "HA Pre-sync: Trying peer %s:%u", host, port);

            if (download_metadata_from_peer(ip, port, data_path) == 0) {
                free(peers_copy);
                return 0;  /* Success! */
            }
        }

        peer = strtok_r(NULL, ",", &saveptr);
    }

    free(peers_copy);
    return -1;
}

/*
 * Pre-metadata synchronization
 * Called BEFORE meta_init() to ensure metadata is available
 * Returns 0 on success (metadata ready), -1 on failure
 */
int ha_pre_metadata_sync(void) {
    uint8_t ha_enabled_cfg;
    char *data_path;
    char *peers_str;
    int result = 0;
    int retry;

    /* Check if HA is enabled */
    ha_enabled_cfg = cfg_getuint8("HA_ENABLED", 0);
    if (!ha_enabled_cfg) {
        /* HA disabled, nothing to do */
        return 0;
    }

    /* Get data path */
    data_path = cfg_getstr("DATA_PATH", "/var/lib/mfs");

    /* Check if metadata already exists */
    if (metadata_file_exists(data_path)) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
                "HA Pre-sync: Metadata file exists, skipping sync");
        free(data_path);
        return 0;
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Pre-sync: No metadata found, will sync from peers");

    /* Get peers from config */
    peers_str = cfg_getstr("HA_PEERS", NULL);
    if (!peers_str || !peers_str[0]) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Pre-sync: No peers configured (HA_PEERS)");
        free(data_path);
        return -1;
    }

    /* Try to sync from peers with retries */
    for (retry = 0; retry < 5; retry++) {
        if (retry > 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
                    "HA Pre-sync: Retry %d/5...", retry + 1);
            sleep(2);
        }

        if (parse_and_try_peers(peers_str, data_path) == 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                    "HA Pre-sync: Metadata synchronized successfully");
            result = 0;
            goto cleanup;
        }
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
            "HA Pre-sync: Failed to sync metadata from any peer after 5 retries");
    result = -1;

cleanup:
    free(data_path);
    free(peers_str);
    return result;
}

/* ============================================================================
 * Callbacks from HA module
 * ============================================================================ */

/*
 * Called when this node becomes the leader
 */
static void on_become_leader_cb(void) {
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: This node is now the LEADER");

    pthread_mutex_lock(&ha_int_mutex);
    cached_leader_ip = 0;  /* We are the leader */
    pthread_mutex_unlock(&ha_int_mutex);

    /*
     * When becoming leader:
     * 1. Enable write operations
     * 2. Start accepting client connections
     * 3. Notify chunkservers of leadership change
     */

    /* The master is already handling clients, just log the transition */
}

/*
 * Called when this node becomes a follower
 */
static void on_become_follower_cb(uint32_t leader_id) {
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: This node is now a FOLLOWER, leader=%u", leader_id);

    pthread_mutex_lock(&ha_int_mutex);
    /* Would need to resolve leader_id to IP - simplified for now */
    cached_leader_ip = leader_id;  /* In real impl, resolve from peer list */
    pthread_mutex_unlock(&ha_int_mutex);

    /*
     * When becoming follower:
     * 1. Stop accepting write operations
     * 2. Redirect writes to leader
     * 3. Continue serving read operations if configured
     */
}

/*
 * Request full metadata sync from leader
 */
static void request_full_sync(void) {
    double now = monotonic_seconds();

    /* Don't spam sync requests - wait at least 5 seconds between requests */
    if (ha_sync_is_in_progress() || (now - last_sync_request_time < 5.0)) {
        return;
    }

    pthread_mutex_lock(&ha_int_mutex);
    sync_needed = 1;
    sync_in_progress = 1;
    last_sync_request_time = now;
    pthread_mutex_unlock(&ha_int_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: Requesting full metadata sync from leader");

    /* Use the new ha_sync module */
    ha_sync_request_full();
}

/*
 * Buffer a changelog for later application after sync
 */
static void buffer_changelog(uint64_t version, const uint8_t *data, uint32_t len) {
    uint32_t next_head;
    changelog_entry_t *entry;

    pthread_mutex_lock(&ha_int_mutex);

    next_head = (changelog_buffer_head + 1) % CHANGELOG_BUFFER_SIZE;
    if (next_head == changelog_buffer_tail) {
        /* Buffer full - drop oldest */
        if (changelog_buffer[changelog_buffer_tail].data != NULL) {
            free(changelog_buffer[changelog_buffer_tail].data);
        }
        changelog_buffer_tail = (changelog_buffer_tail + 1) % CHANGELOG_BUFFER_SIZE;
    }

    entry = &changelog_buffer[changelog_buffer_head];
    entry->version = version;
    entry->len = len;
    entry->data = malloc(len);
    if (entry->data != NULL) {
        memcpy(entry->data, data, len);
        changelog_buffer_head = next_head;
        changelog_buffer_count++;
    }

    pthread_mutex_unlock(&ha_int_mutex);
}

/*
 * Apply buffered changelogs after sync
 */
static void apply_buffered_changelogs(void) {
    uint64_t current_version;
    changelog_entry_t *entry;
    char *changelog_line;
    uint32_t applied = 0;
    uint32_t ts;
    int result;

    current_version = meta_version();

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: Applying %lu buffered changelogs (current version=%lu)",
            changelog_buffer_count, current_version);

    pthread_mutex_lock(&ha_int_mutex);

    while (changelog_buffer_tail != changelog_buffer_head) {
        entry = &changelog_buffer[changelog_buffer_tail];

        if (entry->data != NULL && entry->version == current_version + 1) {
            changelog_line = malloc(entry->len + 1);
            if (changelog_line != NULL) {
                memcpy(changelog_line, entry->data, entry->len);
                changelog_line[entry->len] = '\0';

                pthread_mutex_unlock(&ha_int_mutex);

                /* Write to disk */
                if (changelog_fd != NULL) {
                    fprintf(changelog_fd, "%"PRIu64": %s\n", entry->version, changelog_line);
                    fflush(changelog_fd);
                }

                /* Apply to memory - restore_net expects current_version (before applying) */
                result = restore_net(current_version, changelog_line, &ts);
                if (result == 0) {
                    current_version = entry->version;
                    applied++;
                } else {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                            "HA Integration: Failed to apply buffered changelog %lu",
                            entry->version);
                }

                pthread_mutex_lock(&ha_int_mutex);

                free(changelog_line);
            }
            free(entry->data);
            entry->data = NULL;
        } else if (entry->data != NULL) {
            free(entry->data);
            entry->data = NULL;
        }

        changelog_buffer_tail = (changelog_buffer_tail + 1) % CHANGELOG_BUFFER_SIZE;
    }

    changelog_buffer_count = 0;

    pthread_mutex_unlock(&ha_int_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: Applied %u buffered changelogs, now at version %lu",
            applied, meta_version());
}

/*
 * Called when a changelog entry is received from the leader
 * This implements dual persistence: disk + memory (like metalogger but with live apply)
 */
static void on_changelog_received_cb(uint64_t version, const uint8_t *data, uint32_t len) {
    char *changelog_line;
    uint64_t current_version;
    uint32_t ts;
    int result;

    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
            "HA Integration: Received changelog version %lu, len=%u", version, len);

    /* If sync is in progress, buffer the changelog for later */
    if (ha_sync_is_in_progress()) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                "HA Integration: Sync in progress, buffering changelog %lu", version);
        buffer_changelog(version, data, len);
        return;
    }

    /* Get expected version from metadata */
    current_version = meta_version();

    /* Version check - we should receive changelogs in order */
    if (version <= current_version) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                "HA Integration: Ignoring old changelog version %lu (current=%lu)",
                version, current_version);
        return;
    }

    /* If we're behind by more than 1, we need a full sync */
    if (version > current_version + 1) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Integration: Gap detected! Expected %lu, got %lu - requesting full sync",
                current_version + 1, version);
        buffer_changelog(version, data, len);
        request_full_sync();
        return;
    }

    /* Allocate buffer for null-terminated string */
    changelog_line = malloc(len + 1);
    if (changelog_line == NULL) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Integration: Failed to allocate memory for changelog");
        return;
    }

    memcpy(changelog_line, data, len);
    changelog_line[len] = '\0';

    /*
     * STEP 1: Write to disk for persistence (like metalogger)
     */
    if (changelog_fd == NULL) {
        char path[256];
        snprintf(path, sizeof(path), "%s/changelog_ha.0.mfs",
                 data_path_cache ? data_path_cache : "/var/lib/mfs");
        changelog_fd = fopen(path, "a");
        if (changelog_fd == NULL) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Integration: Cannot open changelog file for writing");
        }
    }

    if (changelog_fd != NULL) {
        fprintf(changelog_fd, "%"PRIu64": %s\n", version, changelog_line);
        fflush(changelog_fd);
    }

    /*
     * STEP 2: Apply to memory using restore_net()
     * This is the key difference from metalogger - we apply LIVE
     * NOTE: restore_net() expects the version BEFORE applying (current_version),
     * not the version of the changelog being applied (version).
     * It will verify: lv == meta_version() before, and lv+1 == meta_version() after.
     */
    result = restore_net(current_version, changelog_line, &ts);

    if (result == 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                "HA Integration: Applied changelog %lu to memory (ts=%u)", version, ts);
    } else {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Integration: Failed to apply changelog %lu (result=%d) - requesting full sync",
                version, result);
        free(changelog_line);
        request_full_sync();
        return;
    }

    free(changelog_line);
}

/*
 * Called when full sync with leader is complete
 */
static void on_sync_complete_cb(void) {
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: Full synchronization with leader complete");

    /* Reset sync state */
    pthread_mutex_lock(&ha_int_mutex);
    sync_in_progress = 0;
    sync_needed = 0;
    pthread_mutex_unlock(&ha_int_mutex);

    /* Apply any buffered changelogs */
    apply_buffered_changelogs();

    /*
     * NOTE: The metadata file has been updated on disk.
     * In a production environment, we would need to reload the metadata
     * into memory. However, MooseFS's meta_restore() may require a restart.
     *
     * For now, log a message indicating a restart may be needed.
     * In a more advanced implementation, we could:
     * 1. Implement hot reload of metadata
     * 2. Signal the main process to restart gracefully
     * 3. Use mmap to share metadata between processes
     */
    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
            "HA Integration: Metadata synced. If version mismatch persists, restart may be needed.");
}

/* ============================================================================
 * Public Functions
 * ============================================================================ */

/*
 * Callback from ha_sync when sync is complete
 */
static void on_ha_sync_complete(int success) {
    if (success) {
        on_sync_complete_cb();
    } else {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Integration: Sync failed!");

        pthread_mutex_lock(&ha_int_mutex);
        sync_in_progress = 0;
        pthread_mutex_unlock(&ha_int_mutex);
    }
}

int ha_integration_init(void) {
    /* Check if HA is enabled */
    ha_enabled = cfg_getuint8("HA_ENABLED", 0);

    if (!ha_enabled) {
        mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO,
                "HA Integration: High availability is disabled");
        return 0;
    }

    /* Cache data path for changelog file */
    data_path_cache = cfg_getstr("DATA_PATH", "/var/lib/mfs");

    /* Initialize sync module */
    if (ha_sync_init() < 0) {
        mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_ERR,
                "HA Integration: Failed to initialize sync module");
        return -1;
    }

    /* Set sync complete callback */
    ha_sync_set_complete_callback(on_ha_sync_complete);

    /* Register callbacks with HA module */
    ha_set_become_leader_callback(on_become_leader_cb);
    ha_set_become_follower_callback(on_become_follower_cb);
    ha_set_changelog_received_callback(on_changelog_received_cb);
    ha_set_sync_complete_callback(on_sync_complete_cb);

    /* Initialize changelog buffer */
    memset(changelog_buffer, 0, sizeof(changelog_buffer));
    changelog_buffer_head = 0;
    changelog_buffer_tail = 0;
    changelog_buffer_count = 0;

    mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO,
            "HA Integration: Initialized with restore_net() for live changelog application");

    return 0;
}

void ha_integration_term(void) {
    /* Close changelog file if open */
    if (changelog_fd != NULL) {
        fclose(changelog_fd);
        changelog_fd = NULL;
    }

    /* Free data path cache */
    if (data_path_cache != NULL) {
        free(data_path_cache);
        data_path_cache = NULL;
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
            "HA Integration: Terminated");
}

int ha_is_enabled(void) {
    return ha_enabled;
}

int ha_can_accept_write(void) {
    if (!ha_enabled) {
        return 1;  /* No HA, always accept writes */
    }

    return ha_is_leader();
}

int ha_should_replicate(void) {
    int is_leader;

    if (!ha_enabled) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                "HA: ha_should_replicate: HA not enabled");
        return 0;  /* No HA, no replication */
    }

    /* Only replicate if we're the leader */
    is_leader = ha_is_leader();
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA: ha_should_replicate: ha_enabled=%d, is_leader=%d",
            ha_enabled, is_leader);
    return is_leader;
}

int ha_replicate_changelog_entry(uint64_t version, const uint8_t *data, uint32_t len) {
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA: ha_replicate_changelog_entry called: version=%lu, len=%u",
            version, len);

    if (!ha_enabled || !ha_is_leader()) {
        return 0;
    }

    return ha_replicate_changelog(version, data, len);
}

void ha_metadata_version_changed(uint64_t version) {
    if (!ha_enabled) {
        return;
    }

    /* Log version change */
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
            "HA Integration: Metadata version changed to %lu", version);

    /* If we're the leader, this is normal after applying our own changes */
    /* If we're a follower, this should only happen when applying replicated changes */
    if (!ha_is_leader()) {
        /* Follower received and applied a change - good */
    }
}

uint32_t ha_get_leader_ip(void) {
    uint32_t ip;

    if (!ha_enabled) {
        return 0;
    }

    pthread_mutex_lock(&ha_int_mutex);
    ip = cached_leader_ip;
    pthread_mutex_unlock(&ha_int_mutex);

    return ip;
}

uint16_t ha_get_leader_port(void) {
    uint16_t port;

    if (!ha_enabled) {
        return 0;
    }

    pthread_mutex_lock(&ha_int_mutex);
    port = cached_leader_port;
    pthread_mutex_unlock(&ha_int_mutex);

    /* Default to standard MooseFS port if not set */
    if (port == 0) {
        port = 9421;
    }

    return port;
}

int ha_wait_for_sync(uint32_t timeout_ms) {
    double start, now;
    double timeout_sec = (double)timeout_ms / 1000.0;

    if (!ha_enabled) {
        return 0;  /* No HA, always "synced" */
    }

    start = monotonic_seconds();

    while (1) {
        if (ha_is_sync_complete()) {
            return 0;
        }

        now = monotonic_seconds();
        if (now - start >= timeout_sec) {
            return -1;  /* Timeout */
        }

        usleep(10000);  /* 10ms */
    }
}

int ha_is_synchronized(void) {
    if (!ha_enabled) {
        return 1;
    }

    return ha_is_sync_complete();
}

int ha_request_leadership(void) {
    if (!ha_enabled) {
        return 0;
    }

    /* This is typically used for administrative purposes */
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: Leadership request received");

    /* In Raft, we can't directly request leadership, but we can
     * step down the current leader which will trigger an election */

    return 0;
}

int ha_step_down_leadership(void) {
    if (!ha_enabled || !ha_is_leader()) {
        return 0;
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Integration: Stepping down from leadership");

    return ha_step_down();
}

void ha_get_status_info(ha_status_info_t *info) {
    ha_cluster_t cluster_status;

    memset(info, 0, sizeof(ha_status_info_t));

    info->enabled = ha_enabled;

    if (!ha_enabled) {
        return;
    }

    ha_get_cluster_status(&cluster_status);

    info->state = (uint8_t)cluster_status.state;
    info->term = cluster_status.current_term;
    info->leader_id = cluster_status.leader_id;
    info->leader_ip = cached_leader_ip;
    info->commit_index = cluster_status.commit_index;
    info->last_applied = cluster_status.last_applied;
    info->peer_count = cluster_status.peer_count;

    /* Count connected peers */
    uint32_t connected = 0;
    for (uint32_t i = 0; i < cluster_status.peer_count; i++) {
        if (cluster_status.peers[i].is_connected) {
            connected++;
        }
    }
    info->connected_peers = connected;
}

const char* ha_format_status(void) {
    ha_status_info_t info;

    ha_get_status_info(&info);

    if (!info.enabled) {
        snprintf(status_buffer, sizeof(status_buffer), "HA: disabled");
    } else {
        const char *state_str;
        switch (info.state) {
            case 0: state_str = "INIT"; break;
            case 1: state_str = "FOLLOWER"; break;
            case 2: state_str = "CANDIDATE"; break;
            case 3: state_str = "LEADER"; break;
            default: state_str = "UNKNOWN"; break;
        }

        snprintf(status_buffer, sizeof(status_buffer),
                "HA: %s, term=%lu, leader=%u, commit=%lu, peers=%u/%u",
                state_str, info.term, info.leader_id,
                info.commit_index, info.connected_peers, info.peer_count);
    }

    return status_buffer;
}

/* ============================================================================
 * CGI/CLI Support Functions
 * ============================================================================ */

/*
 * Get HA info size for CGI protocol
 */
uint32_t ha_get_info_size(void) {
    if (!ha_enabled) {
        return 1;  /* Just enabled flag */
    }
    return 1 + 1 + 8 + 4 + 4 + 8 + 8 + 4 + 4;  /* All fields */
}

/*
 * Fill HA info for CGI protocol
 */
void ha_fill_info(uint8_t *ptr) {
    ha_status_info_t info;

    ha_get_status_info(&info);

    put8bit(&ptr, info.enabled);
    if (info.enabled) {
        put8bit(&ptr, info.state);
        put64bit(&ptr, info.term);
        put32bit(&ptr, info.leader_id);
        put32bit(&ptr, info.leader_ip);
        put64bit(&ptr, info.commit_index);
        put64bit(&ptr, info.last_applied);
        put32bit(&ptr, info.peer_count);
        put32bit(&ptr, info.connected_peers);
    }
}

/* ============================================================================
 * Supervisor Support
 * ============================================================================ */

/*
 * Get HA state for supervisor protocol
 * Returns state code compatible with existing supervisor implementation
 */
uint8_t ha_get_supervisor_state(void) {
    if (!ha_enabled) {
        return 0xFF;  /* HA not enabled, act as standalone */
    }

    switch (ha_get_state()) {
        case HA_STATE_LEADER:
            return 0x01;  /* LEADER */
        case HA_STATE_FOLLOWER:
            return 0x02;  /* FOLLOWER */
        case HA_STATE_CANDIDATE:
            return 0x03;  /* ELECT */
        default:
            return 0x00;  /* INIT/UNKNOWN */
    }
}

/*
 * Check if supervisor should report this node as "master"
 * For clients/chunkservers, only the leader should be reported
 */
int ha_supervisor_is_master(void) {
    if (!ha_enabled) {
        return 1;  /* No HA, always master */
    }

    return ha_is_leader();
}
