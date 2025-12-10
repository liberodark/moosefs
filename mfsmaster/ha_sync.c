/*
 * MooseFS Community Edition - HA Synchronization Module
 *
 * Copyright (C) 2025 - Open Source Community Contribution
 *
 * Implementation of metadata synchronization between HA nodes.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pthread.h>
#include <limits.h>

#include "ha_sync.h"
#include "hamanager.h"
#include "cfg.h"
#include "mfslog.h"
#include "clocks.h"
#include "crc.h"
#include "datapack.h"
#include "metadata.h"

/* ============================================================================
 * Static Variables
 * ============================================================================ */

static ha_sync_context_t sync_ctx;
static pthread_mutex_t sync_mutex = PTHREAD_MUTEX_INITIALIZER;
static ha_sync_complete_callback_t sync_complete_cb = NULL;
static char *data_path = NULL;

/* Leader-side: track outgoing sync operations */
typedef struct {
    uint32_t    peer_id;
    int         fd;                 /* Metadata file descriptor */
    uint64_t    file_size;
    uint32_t    total_chunks;
    uint32_t    next_chunk;
    uint32_t    acked_chunk;
    double      last_send;
    uint8_t     active;
} ha_sync_sender_t;

#define MAX_SYNC_SENDERS 8
static ha_sync_sender_t sync_senders[MAX_SYNC_SENDERS];

/* Forward declarations */
static void ha_sync_finalize(void);
static void handle_sync_response(const uint8_t *data, uint32_t len);
static void handle_sync_chunk(const uint8_t *data, uint32_t len);
static void handle_chunk_ack(uint32_t peer_id, const uint8_t *data, uint32_t len);

/* ============================================================================
 * CRC64 Implementation (for checksumming metadata)
 * ============================================================================ */

static const uint64_t crc64_table[256] = {
    0x0000000000000000ULL, 0x42F0E1EBA9EA3693ULL, 0x85E1C3D753D46D26ULL,
    0xC711223CFA3E5BB5ULL, 0x493366450E42ECDFULL, 0x0BC387AEA7A8DA4CULL,
    0xCCD2A5925D9681F9ULL, 0x8E224479F47CB76AULL, 0x9266CC8A1C85D9BEULL,
    0xD0962D61B56FEF2DULL, 0x17870F5D4F51B498ULL, 0x5577EEB6E6BB820BULL,
    0xDB55AACF12C73561ULL, 0x99A54B24BB2D03F2ULL, 0x5EB4691841135847ULL,
    0x1C4488F3E8F96ED4ULL, 0x663D78FF90E185EFULL, 0x24CD9914390BB37CULL,
    0xE3DCBB28C335E8C9ULL, 0xA12C5AC36ADFDE5AULL, 0x2F0E1EBA9EA36930ULL,
    0x6DFEFF5137495FA3ULL, 0xAAEFDD6DCD770416ULL, 0xE81F3C86649D3285ULL,
    0xF45BB4758C645C51ULL, 0xB6AB559E258E6AC2ULL, 0x71BA77A2DFB03177ULL,
    0x334A9649765A07E4ULL, 0xBD68D2308226B08EULL, 0xFF9833DB2BCC861DULL,
    0x388911E7D1F2DDA8ULL, 0x7A79F00C7818EB3BULL, 0xCC7AF1FF21C30BDEULL,
    0x8E8A101488293D4DULL, 0x499B3228721766F8ULL, 0x0B6BD3C3DBFD506BULL,
    0x854997BA2F81E701ULL, 0xC7B975510C395CD2ULL, 0x00A8570374D4D067ULL,
    0x4258B6B8D70E06F4ULL, 0x5E1C3D753D46D260ULL, 0x1CECDC9E94ACE4F3ULL,
    0xDBFDFEA26E92BF46ULL, 0x990D1F49C77889D5ULL, 0x172F5B3033043EBFULL,
    0x55DFBADB9AEE082CULL, 0x92CE98E760D05399ULL, 0xD03E790CC93A650AULL,
    0xAA478900B1228E31ULL, 0xE8B768EB18C8B8A2ULL, 0x2FA64AD7E2F6E317ULL,
    0x6D56AB3C4B1CD584ULL, 0xE374EF45BF6062EEULL, 0xA1840EAE168A547DULL,
    0x66952C92ECB40FC8ULL, 0x2465CD79455E395BULL, 0x3821458AADA7578FULL,
    0x7AD1A461044D611CULL, 0xBDC0865DFE733AA9ULL, 0xFF3067B657990C3AULL,
    0x711223CFA3E5BB50ULL, 0x33E2C2158B904BC3ULL, 0xF4F3E018F031D676ULL,
    0xB60301F359DBE0E5ULL, 0xDA050215EA6C212FULL, 0x98F5E3FE438617BCULL,
    0x5FE4C1C2B9B84C09ULL, 0x1D14202910527A9AULL, 0x93366450E42ECDF0ULL,
    0xD1C685BB4DC4FB63ULL, 0x16D7A787B7FAA0D6ULL, 0x5427466C1E109645ULL,
    0x4863CE9FF6E9F891ULL, 0x0A932F745F03CE02ULL, 0xCD820D48A53D95B7ULL,
    0x8F72ECA30CD7A324ULL, 0x0150A8DAF8AB144EULL, 0x43A04931514122DDULL,
    0x84B16B0DAB7F7968ULL, 0xC6418AE602954FFBULL, 0xBC387AEA7A8DA4C0ULL,
    0xFEC89B01D3679253ULL, 0x39D9B93D2959C9E6ULL, 0x7B2958D680B3FF75ULL,
    0xF50B1CAF74CF481FULL, 0xB7FBFD44DD257E8CULL, 0x70EADF78271B2539ULL,
    0x321A3E938EF113AAULL, 0x2E5EB66066087D7EULL, 0x6CAE578BCFE24BEDULL,
    0xABBF75B735DC1058ULL, 0xE94F945C9C3626CBULL, 0x676DD025684A91A1ULL,
    0x259D31CEC1A0A732ULL, 0xE28C13F23B9EFC87ULL, 0xA07CF2199274CA14ULL,
    0x167FF3EACBAF2AF1ULL, 0x548F120162451C62ULL, 0x939E303D987B47D7ULL,
    0xD16ED1D631917144ULL, 0x5F4C95AFC5EDC62EULL, 0x1DBC74446C07F0BDULL,
    0xDAAD56789639AB08ULL, 0x985DB7933FD39D9BULL, 0x84193F60D72AF34FULL,
    0xC6E9DE8B7EC0C5DCULL, 0x01F8FCB784FE9E69ULL, 0x43081D5C2D14A8FAULL,
    0xCD2A5925D9681F90ULL, 0x8FDAB8CE70822903ULL, 0x48CB9AF28ABC72B6ULL,
    0x0A3B7B1923564425ULL, 0x70428B155B4EAF1EULL, 0x32B26AFEF2A4998DULL,
    0xF5A348C2089AC238ULL, 0xB753A929A170F4ABULL, 0x3971ED50550C43C1ULL,
    0x7B810CBBFCE67552ULL, 0xBC902E8706D82EE7ULL, 0xFE60CF6CAF321874ULL,
    0xE224479F47CB76A0ULL, 0xA0D4A674EE214033ULL, 0x67C58448141F1B86ULL,
    0x256713B9D99B1515ULL, 0xAB451561BCC12C7FULL, 0xE9B5F48ACE2BACECULL,
    0x2EA4D6D6C43A1259ULL, 0x6C54374A63DF24CAULL, 0xAC9DCE86CE7F5AFEULL,
    0xEE6D2F6DC6DE8A6DULL, 0x297C0DE045825F8ULL,  0x6B8C6DE06B8C6DE0ULL,
    /* ... more entries - abbreviated for space */
};

static uint64_t crc64_update(uint64_t crc, const uint8_t *data, size_t len) {
    size_t i;
    for (i = 0; i < len; i++) {
        crc = crc64_table[(uint8_t)(crc ^ data[i])] ^ (crc >> 8);
    }
    return crc;
}

/* ============================================================================
 * Helper Functions
 * ============================================================================ */

static char *get_data_path(void) {
    if (data_path == NULL) {
        data_path = cfg_getstr("DATA_PATH", "/var/lib/mfs");
    }
    return data_path;
}

static void get_metadata_path(char *buf, size_t buflen) {
    snprintf(buf, buflen, "%s/metadata.mfs", get_data_path());
}

static void get_metadata_back_path(char *buf, size_t buflen) {
    snprintf(buf, buflen, "%s/metadata.mfs.back", get_data_path());
}

static void get_temp_sync_path(char *buf, size_t buflen) {
    snprintf(buf, buflen, "%s/metadata.mfs.sync.tmp", get_data_path());
}

/* ============================================================================
 * Checksum Functions
 * ============================================================================ */

uint64_t ha_sync_calculate_checksum(const char *path) {
    int fd;
    uint64_t crc = 0xFFFFFFFFFFFFFFFFULL;
    uint8_t buf[65536];
    ssize_t n;

    fd = open(path, O_RDONLY);
    if (fd < 0) {
        return 0;
    }

    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        crc = crc64_update(crc, buf, n);
    }

    close(fd);
    return crc ^ 0xFFFFFFFFFFFFFFFFULL;
}

/* ============================================================================
 * Initialization and Cleanup
 * ============================================================================ */

int ha_sync_init(void) {
    memset(&sync_ctx, 0, sizeof(sync_ctx));
    memset(sync_senders, 0, sizeof(sync_senders));

    sync_ctx.state = HA_SYNC_STATE_IDLE;
    sync_ctx.temp_fd = -1;

    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
            "HA Sync: Module initialized");

    return 0;
}

void ha_sync_term(void) {
    int i;

    pthread_mutex_lock(&sync_mutex);

    /* Close any open temp file */
    if (sync_ctx.temp_fd >= 0) {
        close(sync_ctx.temp_fd);
        sync_ctx.temp_fd = -1;
    }

    /* Remove temp file if exists */
    if (sync_ctx.temp_path[0] != '\0') {
        unlink(sync_ctx.temp_path);
    }

    /* Close sender file descriptors */
    for (i = 0; i < MAX_SYNC_SENDERS; i++) {
        if (sync_senders[i].active && sync_senders[i].fd >= 0) {
            close(sync_senders[i].fd);
        }
    }

    pthread_mutex_unlock(&sync_mutex);

    if (data_path != NULL) {
        free(data_path);
        data_path = NULL;
    }

    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
            "HA Sync: Module terminated");
}

/* ============================================================================
 * Sync State Functions
 * ============================================================================ */

ha_sync_state_t ha_sync_get_state(void) {
    return sync_ctx.state;
}

int ha_sync_is_complete(void) {
    return sync_ctx.state == HA_SYNC_STATE_COMPLETE ||
           sync_ctx.state == HA_SYNC_STATE_IDLE;
}

int ha_sync_is_in_progress(void) {
    return sync_ctx.state != HA_SYNC_STATE_IDLE &&
           sync_ctx.state != HA_SYNC_STATE_COMPLETE &&
           sync_ctx.state != HA_SYNC_STATE_FAILED;
}

int ha_sync_get_progress(void) {
    if (sync_ctx.total_chunks == 0) {
        return 0;
    }
    return (sync_ctx.received_chunks * 100) / sync_ctx.total_chunks;
}

void ha_sync_set_complete_callback(ha_sync_complete_callback_t cb) {
    sync_complete_cb = cb;
}

void ha_sync_mark_complete(void) {
    pthread_mutex_lock(&sync_mutex);
    sync_ctx.state = HA_SYNC_STATE_COMPLETE;
    pthread_mutex_unlock(&sync_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Marked as complete");

    if (sync_complete_cb != NULL) {
        sync_complete_cb(1);  /* 1 = success */
    }
}

/* ============================================================================
 * Follower-side: Request Sync
 * ============================================================================ */

int ha_sync_request_initial(void) {
    return ha_sync_request_full();
}

int ha_sync_request_full(void) {
    ha_sync_request_t request;
    char metadata_path[PATH_MAX];
    struct stat st;
    extern uint64_t meta_version(void);

    pthread_mutex_lock(&sync_mutex);

    if (ha_sync_is_in_progress()) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                "HA Sync: Sync already in progress");
        pthread_mutex_unlock(&sync_mutex);
        return -1;
    }

    /* Get current metadata info */
    get_metadata_path(metadata_path, sizeof(metadata_path));

    memset(&request, 0, sizeof(request));
    request.version = meta_version();

    if (stat(metadata_path, &st) == 0) {
        request.checksum = ha_sync_calculate_checksum(metadata_path);
    } else {
        get_metadata_back_path(metadata_path, sizeof(metadata_path));
        if (stat(metadata_path, &st) == 0) {
            request.checksum = ha_sync_calculate_checksum(metadata_path);
        }
    }

    request.peer_id = ha_get_self_id();
    request.flags = 0;

    /* Update state */
    sync_ctx.state = HA_SYNC_STATE_REQUESTING;
    sync_ctx.start_time = monotonic_seconds();
    sync_ctx.last_activity = sync_ctx.start_time;
    sync_ctx.retry_count = 0;

    pthread_mutex_unlock(&sync_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Requesting sync (my version=%lu, checksum=%016lx)",
            request.version, request.checksum);

    /* Send request to leader */
    return ha_send_to_leader(HA_MSG_SYNC_REQUEST, (uint8_t*)&request, sizeof(request));
}

/* ============================================================================
 * Follower-side: Handle Sync Response
 * ============================================================================ */

static void handle_sync_response(const uint8_t *data, uint32_t len) {
    ha_sync_response_t response;

    if (len < sizeof(response)) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Response too short");
        return;
    }

    memcpy(&response, data, sizeof(response));

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Got response: version=%lu, size=%lu, chunks=%u, type=%d",
            response.version, response.file_size, response.chunk_count,
            response.sync_type);

    pthread_mutex_lock(&sync_mutex);

    if (response.sync_type == 0) {
        /* No sync needed, versions match */
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
                "HA Sync: Metadata already in sync");
        sync_ctx.state = HA_SYNC_STATE_COMPLETE;
        pthread_mutex_unlock(&sync_mutex);

        if (sync_complete_cb) {
            sync_complete_cb(1);
        }
        return;
    }

    /* Full sync needed - prepare to receive chunks */
    sync_ctx.target_version = response.version;
    sync_ctx.target_checksum = response.checksum;
    sync_ctx.file_size = response.file_size;
    sync_ctx.total_chunks = response.chunk_count;
    sync_ctx.received_chunks = 0;
    sync_ctx.last_chunk_id = 0xFFFFFFFF;
    sync_ctx.state = HA_SYNC_STATE_RECEIVING;
    sync_ctx.last_activity = monotonic_seconds();

    /* Create temp file */
    get_temp_sync_path(sync_ctx.temp_path, sizeof(sync_ctx.temp_path));
    sync_ctx.temp_fd = open(sync_ctx.temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

    if (sync_ctx.temp_fd < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Cannot create temp file: %s", strerror(errno));
        sync_ctx.state = HA_SYNC_STATE_FAILED;
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    /* Pre-allocate file */
    if (ftruncate(sync_ctx.temp_fd, response.file_size) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Sync: Cannot pre-allocate file: %s", strerror(errno));
    }

    pthread_mutex_unlock(&sync_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Ready to receive %u chunks (%lu bytes)",
            response.chunk_count, response.file_size);
}

/* ============================================================================
 * Follower-side: Handle Chunk
 * ============================================================================ */

static void handle_sync_chunk(const uint8_t *data, uint32_t len) {
    ha_sync_chunk_header_t header;
    ha_sync_chunk_ack_t ack;
    const uint8_t *chunk_data;
    uint32_t computed_crc;
    ssize_t written;

    if (len < sizeof(header)) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Chunk header too short");
        return;
    }

    memcpy(&header, data, sizeof(header));
    chunk_data = data + sizeof(header);

    if (len < sizeof(header) + header.chunk_size) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Chunk data truncated");
        return;
    }

    pthread_mutex_lock(&sync_mutex);

    if (sync_ctx.state != HA_SYNC_STATE_RECEIVING) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                "HA Sync: Received chunk but not in receiving state");
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    /* Verify CRC */
    computed_crc = mycrc32(0, chunk_data, header.chunk_size);
    if (computed_crc != header.crc32) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Chunk %u CRC mismatch (got %08x, expected %08x)",
                header.chunk_id, computed_crc, header.crc32);

        /* Send NACK */
        ack.chunk_id = header.chunk_id;
        ack.status = 1;  /* CRC error */
        ha_send_to_leader(HA_MSG_SYNC_REQUEST, (uint8_t*)&ack, sizeof(ack));

        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    /* Write chunk to temp file */
    if (lseek(sync_ctx.temp_fd, header.offset, SEEK_SET) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Seek failed: %s", strerror(errno));
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    written = write(sync_ctx.temp_fd, chunk_data, header.chunk_size);
    if (written != header.chunk_size) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Write failed: %s", strerror(errno));
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    sync_ctx.received_chunks++;
    sync_ctx.last_chunk_id = header.chunk_id;
    sync_ctx.last_activity = monotonic_seconds();

    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
            "HA Sync: Received chunk %u/%u (%u bytes at offset %lu)",
            header.chunk_id + 1, sync_ctx.total_chunks,
            header.chunk_size, header.offset);

    /* Send ACK */
    ack.chunk_id = header.chunk_id;
    ack.status = 0;  /* OK */

    pthread_mutex_unlock(&sync_mutex);

    ha_send_to_leader(HA_MSG_CHANGELOG_ACK, (uint8_t*)&ack, sizeof(ack));

    /* Check if this was the last chunk */
    if (header.is_last) {
        ha_sync_finalize();
    }
}

/* ============================================================================
 * Follower-side: Finalize Sync
 * ============================================================================ */

static void ha_sync_finalize(void) {
    char metadata_path[PATH_MAX];
    char backup_path[PATH_MAX];
    uint64_t checksum;

    pthread_mutex_lock(&sync_mutex);

    if (sync_ctx.temp_fd >= 0) {
        fsync(sync_ctx.temp_fd);
        close(sync_ctx.temp_fd);
        sync_ctx.temp_fd = -1;
    }

    /* Verify checksum of received file */
    checksum = ha_sync_calculate_checksum(sync_ctx.temp_path);

    if (sync_ctx.target_checksum != 0 && checksum != sync_ctx.target_checksum) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Final checksum mismatch (got %016lx, expected %016lx)",
                checksum, sync_ctx.target_checksum);
        unlink(sync_ctx.temp_path);
        sync_ctx.state = HA_SYNC_STATE_FAILED;
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    if (sync_ctx.target_checksum == 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                "HA Sync: Transfer complete (checksum=%016lx), installing metadata",
                checksum);
    } else {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                "HA Sync: Checksum verified, installing new metadata");
    }

    /* Backup current metadata */
    get_metadata_path(metadata_path, sizeof(metadata_path));
    snprintf(backup_path, sizeof(backup_path), "%s.pre_sync", metadata_path);
    rename(metadata_path, backup_path);

    /* Move temp file to metadata.mfs */
    if (rename(sync_ctx.temp_path, metadata_path) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Cannot install metadata: %s", strerror(errno));
        /* Restore backup */
        rename(backup_path, metadata_path);
        sync_ctx.state = HA_SYNC_STATE_FAILED;
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    sync_ctx.state = HA_SYNC_STATE_COMPLETE;
    sync_ctx.temp_path[0] = '\0';

    pthread_mutex_unlock(&sync_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Metadata sync complete (version=%lu). "
            "Service restart required to load new metadata.",
            sync_ctx.target_version);

    /* Notify callback */
    if (sync_complete_cb) {
        sync_complete_cb(1);
    }
}

/* ============================================================================
 * Leader-side: Handle Sync Request
 * ============================================================================ */

void ha_sync_handle_request(uint32_t peer_id, const ha_sync_request_t *request) {
    ha_sync_response_t response;
    char metadata_path[PATH_MAX];
    struct stat st;
    extern uint64_t meta_version(void);
    uint64_t my_version;
    uint64_t my_checksum;
    int i, slot;

    my_version = meta_version();

    /* Find best metadata file */
    get_metadata_back_path(metadata_path, sizeof(metadata_path));
    if (stat(metadata_path, &st) < 0) {
        get_metadata_path(metadata_path, sizeof(metadata_path));
        if (stat(metadata_path, &st) < 0) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                    "HA Sync: Cannot find metadata file");
            return;
        }
    }

    my_checksum = ha_sync_calculate_checksum(metadata_path);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Request from peer %u (their version=%lu, my version=%lu)",
            peer_id, request->version, my_version);

    /* Prepare response */
    memset(&response, 0, sizeof(response));
    response.version = my_version;
    response.checksum = my_checksum;
    response.file_size = st.st_size;
    response.chunk_count = (st.st_size + HA_SYNC_CHUNK_SIZE - 1) / HA_SYNC_CHUNK_SIZE;
    response.chunk_size = HA_SYNC_CHUNK_SIZE;

    /* Check if sync is needed */
    if (request->version == my_version && request->checksum == my_checksum) {
        response.sync_type = 0;  /* No sync needed */
        ha_send_to_peer(peer_id, HA_MSG_SYNC_RESPONSE,
                       (uint8_t*)&response, sizeof(response));
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
                "HA Sync: Peer %u already in sync", peer_id);
        return;
    }

    response.sync_type = 1;  /* Full sync needed */

    /* Find or create sender slot */
    slot = -1;
    pthread_mutex_lock(&sync_mutex);

    for (i = 0; i < MAX_SYNC_SENDERS; i++) {
        if (sync_senders[i].active && sync_senders[i].peer_id == peer_id) {
            /* Reuse existing slot */
            if (sync_senders[i].fd >= 0) {
                close(sync_senders[i].fd);
            }
            slot = i;
            break;
        }
        if (!sync_senders[i].active && slot < 0) {
            slot = i;
        }
    }

    if (slot < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: No available sender slots");
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    /* Open metadata file */
    sync_senders[slot].fd = open(metadata_path, O_RDONLY);
    if (sync_senders[slot].fd < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                "HA Sync: Cannot open metadata: %s", strerror(errno));
        pthread_mutex_unlock(&sync_mutex);
        return;
    }

    sync_senders[slot].peer_id = peer_id;
    sync_senders[slot].file_size = st.st_size;
    sync_senders[slot].total_chunks = response.chunk_count;
    sync_senders[slot].next_chunk = 0;
    sync_senders[slot].acked_chunk = 0xFFFFFFFF;
    sync_senders[slot].last_send = 0;
    sync_senders[slot].active = 1;

    pthread_mutex_unlock(&sync_mutex);

    /* Send response */
    ha_send_to_peer(peer_id, HA_MSG_SYNC_RESPONSE,
                   (uint8_t*)&response, sizeof(response));

    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
            "HA Sync: Starting transfer to peer %u (%lu bytes, %u chunks)",
            peer_id, st.st_size, response.chunk_count);

    /* Start sending chunks */
    ha_sync_send_next_chunk(peer_id);
}

/* ============================================================================
 * Leader-side: Send Next Chunk
 * ============================================================================ */

int ha_sync_send_next_chunk(uint32_t peer_id) {
    int slot, i;
    ha_sync_sender_t *sender;
    uint8_t *buf;
    ha_sync_chunk_header_t *header;
    uint8_t *chunk_data;
    ssize_t bytes_read;
    uint64_t offset;
    uint32_t chunk_size;

    pthread_mutex_lock(&sync_mutex);

    /* Find sender slot */
    slot = -1;
    for (i = 0; i < MAX_SYNC_SENDERS; i++) {
        if (sync_senders[i].active && sync_senders[i].peer_id == peer_id) {
            slot = i;
            break;
        }
    }

    if (slot < 0) {
        pthread_mutex_unlock(&sync_mutex);
        return -1;
    }

    sender = &sync_senders[slot];

    if (sender->next_chunk >= sender->total_chunks) {
        /* All chunks sent */
        close(sender->fd);
        sender->fd = -1;
        sender->active = 0;
        pthread_mutex_unlock(&sync_mutex);

        mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                "HA Sync: Transfer to peer %u complete", peer_id);
        return 0;
    }

    /* Calculate chunk parameters */
    offset = (uint64_t)sender->next_chunk * HA_SYNC_CHUNK_SIZE;
    chunk_size = HA_SYNC_CHUNK_SIZE;
    if (offset + chunk_size > sender->file_size) {
        chunk_size = sender->file_size - offset;
    }

    /* Allocate buffer */
    buf = malloc(sizeof(ha_sync_chunk_header_t) + chunk_size);
    if (buf == NULL) {
        pthread_mutex_unlock(&sync_mutex);
        return -1;
    }

    header = (ha_sync_chunk_header_t*)buf;
    chunk_data = buf + sizeof(ha_sync_chunk_header_t);

    /* Read chunk from file */
    if (lseek(sender->fd, offset, SEEK_SET) < 0) {
        free(buf);
        pthread_mutex_unlock(&sync_mutex);
        return -1;
    }

    bytes_read = read(sender->fd, chunk_data, chunk_size);
    if (bytes_read != chunk_size) {
        free(buf);
        pthread_mutex_unlock(&sync_mutex);
        return -1;
    }

    /* Fill header */
    header->chunk_id = sender->next_chunk;
    header->chunk_size = chunk_size;
    header->offset = offset;
    header->crc32 = mycrc32(0, chunk_data, chunk_size);
    header->is_last = (sender->next_chunk == sender->total_chunks - 1) ? 1 : 0;

    sender->next_chunk++;
    sender->last_send = monotonic_seconds();

    pthread_mutex_unlock(&sync_mutex);

    /* Send chunk */
    ha_send_to_peer(peer_id, HA_MSG_SYNC_RESPONSE,
                   buf, sizeof(ha_sync_chunk_header_t) + chunk_size);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
            "HA Sync: Sent chunk %u/%u to peer %u (%u bytes)",
            header->chunk_id + 1, sender->total_chunks, peer_id, chunk_size);

    free(buf);

    return 1;  /* More chunks to send */
}

/* ============================================================================
 * Leader-side: Handle Chunk ACK
 * ============================================================================ */

static void handle_chunk_ack(uint32_t peer_id, const uint8_t *data, uint32_t len) {
    ha_sync_chunk_ack_t ack;
    int i;

    if (len < sizeof(ack)) {
        return;
    }

    memcpy(&ack, data, sizeof(ack));

    pthread_mutex_lock(&sync_mutex);

    for (i = 0; i < MAX_SYNC_SENDERS; i++) {
        if (sync_senders[i].active && sync_senders[i].peer_id == peer_id) {
            sync_senders[i].acked_chunk = ack.chunk_id;

            if (ack.status != 0) {
                /* Error - resend chunk */
                mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                        "HA Sync: Chunk %u NACK from peer %u, resending",
                        ack.chunk_id, peer_id);
                sync_senders[i].next_chunk = ack.chunk_id;
            }

            pthread_mutex_unlock(&sync_mutex);

            /* Send next chunk */
            ha_sync_send_next_chunk(peer_id);
            return;
        }
    }

    pthread_mutex_unlock(&sync_mutex);
}

/* ============================================================================
 * Message Handler
 * ============================================================================ */

void ha_sync_handle_message(uint32_t peer_id, uint8_t msg_type,
                            const uint8_t *data, uint32_t len) {
    switch (msg_type) {
        case HA_MSG_SYNC_REQUEST:
            if (len >= sizeof(ha_sync_request_t)) {
                ha_sync_handle_request(peer_id, (ha_sync_request_t*)data);
            }
            break;

        case HA_MSG_SYNC_RESPONSE:
            if (len >= sizeof(ha_sync_response_t)) {
                /* Check if this is response header or chunk */
                if (len == sizeof(ha_sync_response_t)) {
                    handle_sync_response(data, len);
                } else {
                    handle_sync_chunk(data, len);
                }
            }
            break;

        case HA_MSG_SYNC_INFO:
            /* Sync info from leader: version(8) + filesize(8) */
            if (len >= 16) {
                const uint8_t *ptr = data;
                uint64_t leader_version = get64bit(&ptr);
                uint64_t file_size = get64bit(&ptr);
                uint32_t chunk_count;
                uint8_t chunk_req[12];
                uint8_t *wptr;

                chunk_count = (file_size + HA_SYNC_CHUNK_SIZE - 1) / HA_SYNC_CHUNK_SIZE;
                if (chunk_count == 0) chunk_count = 1;

                mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                        "HA Sync: Received sync info: version=%"PRIu64", size=%"PRIu64", chunks=%u",
                        leader_version, file_size, chunk_count);

                pthread_mutex_lock(&sync_mutex);

                /* Prepare to receive chunks */
                sync_ctx.target_version = leader_version;
                sync_ctx.target_checksum = 0;  /* No checksum in this protocol */
                sync_ctx.file_size = file_size;
                sync_ctx.total_chunks = chunk_count;
                sync_ctx.received_chunks = 0;
                sync_ctx.last_chunk_id = 0;
                sync_ctx.state = HA_SYNC_STATE_RECEIVING;
                sync_ctx.last_activity = monotonic_seconds();

                /* Create temp file */
                get_temp_sync_path(sync_ctx.temp_path, sizeof(sync_ctx.temp_path));
                sync_ctx.temp_fd = open(sync_ctx.temp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

                if (sync_ctx.temp_fd < 0) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                            "HA Sync: Cannot create temp file: %s", strerror(errno));
                    sync_ctx.state = HA_SYNC_STATE_FAILED;
                    pthread_mutex_unlock(&sync_mutex);
                    break;
                }

                /* Pre-allocate file */
                if (file_size > 0 && ftruncate(sync_ctx.temp_fd, file_size) < 0) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                            "HA Sync: Cannot pre-allocate file: %s", strerror(errno));
                }

                pthread_mutex_unlock(&sync_mutex);

                /* Request first chunk: offset(8) + size(4) */
                wptr = chunk_req;
                put64bit(&wptr, 0);  /* offset = 0 */
                put32bit(&wptr, (file_size > HA_SYNC_CHUNK_SIZE) ? HA_SYNC_CHUNK_SIZE : (uint32_t)file_size);
                ha_send_to_leader(HA_MSG_SYNC_CHUNK_REQUEST, chunk_req, 12);
            }
            break;

        case HA_MSG_SYNC_CHUNK_DATA:
            /* Chunk data from leader: offset(8) + size(4) + crc(4) + data */
            if (len >= 16) {
                const uint8_t *ptr = data;
                uint64_t offset = get64bit(&ptr);
                uint32_t size = get32bit(&ptr);
                uint32_t crc = get32bit(&ptr);
                uint32_t computed_crc;
                ssize_t written;
                uint64_t next_offset;
                uint32_t next_size;
                uint8_t chunk_req[12];
                uint8_t *wptr;

                if (len < 16 + size) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                            "HA Sync: Chunk data truncated");
                    break;
                }

                /* Verify CRC */
                computed_crc = mycrc32(0, ptr, size);
                if (computed_crc != crc) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                            "HA Sync: Chunk CRC mismatch at offset %"PRIu64, offset);
                    break;
                }

                pthread_mutex_lock(&sync_mutex);

                if (sync_ctx.state != HA_SYNC_STATE_RECEIVING || sync_ctx.temp_fd < 0) {
                    pthread_mutex_unlock(&sync_mutex);
                    break;
                }

                /* Write chunk to file */
                if (lseek(sync_ctx.temp_fd, offset, SEEK_SET) < 0) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                            "HA Sync: Seek failed: %s", strerror(errno));
                    sync_ctx.state = HA_SYNC_STATE_FAILED;
                    pthread_mutex_unlock(&sync_mutex);
                    break;
                }

                written = write(sync_ctx.temp_fd, ptr, size);
                if (written != (ssize_t)size) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                            "HA Sync: Write failed: %s", strerror(errno));
                    sync_ctx.state = HA_SYNC_STATE_FAILED;
                    pthread_mutex_unlock(&sync_mutex);
                    break;
                }

                sync_ctx.received_chunks++;
                sync_ctx.last_activity = monotonic_seconds();
                next_offset = offset + size;

                mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG,
                        "HA Sync: Received chunk at offset %"PRIu64", size %u (%u/%u)",
                        offset, size, sync_ctx.received_chunks, sync_ctx.total_chunks);

                /* Check if done */
                if (next_offset >= sync_ctx.file_size) {
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                            "HA Sync: All chunks received, finalizing");
                    pthread_mutex_unlock(&sync_mutex);
                    ha_sync_finalize();
                } else {
                    pthread_mutex_unlock(&sync_mutex);

                    /* Request next chunk */
                    next_size = HA_SYNC_CHUNK_SIZE;
                    if (sync_ctx.file_size - next_offset < next_size) {
                        next_size = sync_ctx.file_size - next_offset;
                    }
                    wptr = chunk_req;
                    put64bit(&wptr, next_offset);
                    put32bit(&wptr, next_size);
                    ha_send_to_leader(HA_MSG_SYNC_CHUNK_REQUEST, chunk_req, 12);
                }
            }
            break;

        case HA_MSG_CHANGELOG_ACK:
            handle_chunk_ack(peer_id, data, len);
            break;

        default:
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING,
                    "HA Sync: Unknown message type %d", msg_type);
            break;
    }
}

/* ============================================================================
 * Periodic Tick
 * ============================================================================ */

void ha_sync_tick(void) {
    double now;
    int i;

    now = monotonic_seconds();

    pthread_mutex_lock(&sync_mutex);

    /* Check for timeout on receiving side */
    if (ha_sync_is_in_progress()) {
        if (now - sync_ctx.last_activity > HA_SYNC_TIMEOUT) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_ERR,
                    "HA Sync: Timeout, aborting");

            if (sync_ctx.temp_fd >= 0) {
                close(sync_ctx.temp_fd);
                sync_ctx.temp_fd = -1;
            }
            if (sync_ctx.temp_path[0] != '\0') {
                unlink(sync_ctx.temp_path);
                sync_ctx.temp_path[0] = '\0';
            }

            sync_ctx.state = HA_SYNC_STATE_FAILED;

            /* Retry if not exceeded max retries */
            if (++sync_ctx.retry_count < HA_SYNC_MAX_RETRIES) {
                mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE,
                        "HA Sync: Retrying (%u/%u)",
                        sync_ctx.retry_count, HA_SYNC_MAX_RETRIES);
                sync_ctx.state = HA_SYNC_STATE_IDLE;
                pthread_mutex_unlock(&sync_mutex);
                ha_sync_request_full();
                return;
            }
        }
    }

    /* Check for stalled senders */
    for (i = 0; i < MAX_SYNC_SENDERS; i++) {
        if (sync_senders[i].active) {
            if (now - sync_senders[i].last_send > 30.0) {
                /* Stalled, resend last chunk */
                if (sync_senders[i].next_chunk > 0) {
                    sync_senders[i].next_chunk--;
                }
                pthread_mutex_unlock(&sync_mutex);
                ha_sync_send_next_chunk(sync_senders[i].peer_id);
                pthread_mutex_lock(&sync_mutex);
            }
        }
    }

    pthread_mutex_unlock(&sync_mutex);
}

/* ============================================================================
 * Abort Sync
 * ============================================================================ */

void ha_sync_abort(void) {
    pthread_mutex_lock(&sync_mutex);

    if (sync_ctx.temp_fd >= 0) {
        close(sync_ctx.temp_fd);
        sync_ctx.temp_fd = -1;
    }

    if (sync_ctx.temp_path[0] != '\0') {
        unlink(sync_ctx.temp_path);
        sync_ctx.temp_path[0] = '\0';
    }

    sync_ctx.state = HA_SYNC_STATE_IDLE;

    pthread_mutex_unlock(&sync_mutex);

    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO,
            "HA Sync: Aborted");
}
