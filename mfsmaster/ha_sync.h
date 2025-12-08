/*
 * MooseFS Community Edition - HA Synchronization Module
 *
 * Copyright (C) 2025 - Open Source Community Contribution
 *
 * This module handles metadata synchronization between HA nodes:
 * - Initial sync when a follower joins
 * - Full sync when gaps are detected
 * - Incremental sync via changelog replication
 * - Chunked transfer for large metadata files
 */

#ifndef _HA_SYNC_H_
#define _HA_SYNC_H_

#include <stdint.h>

/* ============================================================================
 * Constants
 * ============================================================================ */

/* Sync chunk size: 1MB for efficient transfer */
#define HA_SYNC_CHUNK_SIZE      (1024 * 1024)

/* Maximum metadata file size: 10GB */
#define HA_SYNC_MAX_FILE_SIZE   (10ULL * 1024 * 1024 * 1024)

/* Sync timeout in seconds */
#define HA_SYNC_TIMEOUT         300

/* Maximum retries for chunk transfer */
#define HA_SYNC_MAX_RETRIES     3

/* Sync states */
typedef enum {
    HA_SYNC_STATE_IDLE = 0,
    HA_SYNC_STATE_REQUESTING,       /* Sent sync request, waiting for response */
    HA_SYNC_STATE_RECEIVING,        /* Receiving metadata chunks */
    HA_SYNC_STATE_WRITING,          /* Writing received data to disk */
    HA_SYNC_STATE_RELOADING,        /* Reloading metadata into memory */
    HA_SYNC_STATE_COMPLETE,         /* Sync completed successfully */
    HA_SYNC_STATE_FAILED            /* Sync failed */
} ha_sync_state_t;

/* Sync message types (sub-types of HA_MSG_SYNC_*) */
typedef enum {
    HA_SYNC_MSG_REQUEST = 1,        /* Request sync with version info */
    HA_SYNC_MSG_RESPONSE_OK,        /* Sync not needed, versions match */
    HA_SYNC_MSG_RESPONSE_FULL,      /* Full sync needed, sending metadata */
    HA_SYNC_MSG_CHUNK,              /* Metadata chunk */
    HA_SYNC_MSG_CHUNK_ACK,          /* Chunk acknowledgment */
    HA_SYNC_MSG_COMPLETE,           /* Transfer complete */
    HA_SYNC_MSG_ABORT,              /* Abort transfer */
    HA_SYNC_MSG_CHANGELOG_BATCH     /* Batch of missing changelogs */
} ha_sync_msg_type_t;

/* ============================================================================
 * Data Structures
 * ============================================================================ */

/* Sync request payload */
typedef struct {
    uint64_t    version;            /* Current metadata version */
    uint64_t    checksum;           /* Metadata checksum (CRC64) */
    uint32_t    peer_id;            /* Requesting peer ID */
    uint32_t    flags;              /* Request flags */
} __attribute__((packed)) ha_sync_request_t;

/* Sync response header */
typedef struct {
    uint64_t    version;            /* Leader's metadata version */
    uint64_t    checksum;           /* Leader's metadata checksum */
    uint64_t    file_size;          /* Total metadata file size */
    uint32_t    chunk_count;        /* Number of chunks to transfer */
    uint32_t    chunk_size;         /* Size of each chunk */
    uint8_t     sync_type;          /* 0=not needed, 1=full, 2=incremental */
    uint8_t     reserved[3];        /* Alignment padding */
} __attribute__((packed)) ha_sync_response_t;

/* Chunk header */
typedef struct {
    uint32_t    chunk_id;           /* Chunk sequence number (0-based) */
    uint32_t    chunk_size;         /* Size of this chunk */
    uint64_t    offset;             /* Offset in file */
    uint32_t    crc32;              /* CRC32 of chunk data */
    uint8_t     is_last;            /* 1 if this is the last chunk */
    uint8_t     reserved[3];        /* Alignment padding */
} __attribute__((packed)) ha_sync_chunk_header_t;

/* Chunk ACK */
typedef struct {
    uint32_t    chunk_id;           /* Acknowledged chunk ID */
    uint8_t     status;             /* 0=OK, 1=CRC error, 2=other error */
    uint8_t     reserved[3];        /* Alignment padding */
} __attribute__((packed)) ha_sync_chunk_ack_t;

/* Sync context (tracks ongoing sync operation) */
typedef struct {
    ha_sync_state_t state;          /* Current sync state */
    uint64_t    target_version;     /* Target version we're syncing to */
    uint64_t    target_checksum;    /* Expected checksum */
    uint64_t    file_size;          /* Total file size */
    uint32_t    total_chunks;       /* Total number of chunks */
    uint32_t    received_chunks;    /* Number of chunks received */
    uint32_t    last_chunk_id;      /* Last received chunk ID */
    double      start_time;         /* Sync start time */
    double      last_activity;      /* Last activity timestamp */
    int         temp_fd;            /* Temp file descriptor */
    char        temp_path[256];     /* Temp file path */
    uint32_t    retry_count;        /* Retry counter */
    uint32_t    peer_id;            /* Peer we're syncing with */
} ha_sync_context_t;

/* ============================================================================
 * Public Functions
 * ============================================================================ */

/* Initialize sync module */
int ha_sync_init(void);

/* Cleanup sync module */
void ha_sync_term(void);

/* Request initial sync (called on follower startup) */
int ha_sync_request_initial(void);

/* Request full sync (called when gap detected) */
int ha_sync_request_full(void);

/* Handle incoming sync message */
void ha_sync_handle_message(uint32_t peer_id, uint8_t msg_type,
                            const uint8_t *data, uint32_t len);

/* Check sync status */
ha_sync_state_t ha_sync_get_state(void);
int ha_sync_is_complete(void);
int ha_sync_is_in_progress(void);

/* Get sync progress (0-100%) */
int ha_sync_get_progress(void);

/* Abort ongoing sync */
void ha_sync_abort(void);

/* Periodic sync tick (check timeouts, etc.) */
void ha_sync_tick(void);

/* Calculate metadata checksum */
uint64_t ha_sync_calculate_checksum(const char *path);

/* Leader-side: handle sync request from follower */
void ha_sync_handle_request(uint32_t peer_id, const ha_sync_request_t *request);

/* Leader-side: send next chunk to follower */
int ha_sync_send_next_chunk(uint32_t peer_id);

/* Callback when sync completes - caller should reload metadata */
typedef void (*ha_sync_complete_callback_t)(int success);
void ha_sync_set_complete_callback(ha_sync_complete_callback_t cb);

#endif /* _HA_SYNC_H_ */
