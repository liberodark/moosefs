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

#include "ha_integration.h"
#include "hamanager.h"
#include "cfg.h"
#include "main.h"
#include "mfslog.h"
#include "metadata.h"
#include "changelog.h"
#include "matoclserv.h"
#include "clocks.h"
#include "datapack.h"

/* External MooseFS functions for changelog handling */
/* changelog_mr is declared in changelog.h */
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
 * Called when a changelog entry is received from the leader
 */
static void on_changelog_received_cb(uint64_t version, const uint8_t *data, uint32_t len) {
    char *changelog_line;
    uint64_t expected_version;
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, 
            "HA Integration: Received changelog version %lu, len=%u", version, len);
    
    /*
     * Apply the changelog entry to our local metadata
     * The data is a changelog line in text format: "OPERATION(params...)"
     */
    
    /* Get expected version from metadata */
    expected_version = meta_version();
    
    /* Version check - we should receive changelogs in order */
    if (version != expected_version + 1) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA Integration: Version mismatch! Expected %lu, got %lu",
                expected_version + 1, version);
        
        /* If we're behind, we need a full sync */
        if (version > expected_version + 1) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                    "HA Integration: Gap detected, need full sync");
            /* TODO: Request full metadata sync from leader */
        }
        /* If we're ahead, ignore (duplicate or old message) */
        if (version <= expected_version) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, 
                    "HA Integration: Ignoring old changelog version %lu", version);
            return;
        }
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
    
    /* Log the changelog entry for debugging */
    mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, 
            "HA Integration: Applying changelog: %lu: %s", version, changelog_line);
    
    /* Apply the changelog using MooseFS's restore function */
    /* changelog_mr() parses and applies the changelog entry to metadata */
    changelog_mr(version, changelog_line);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
            "HA Integration: Applied changelog version %lu", version);
    
    free(changelog_line);
}

/*
 * Called when full sync with leader is complete
 */
static void on_sync_complete_cb(void) {
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
            "HA Integration: Full synchronization with leader complete");
    
    /* Mark node as synchronized and ready */
}

/* ============================================================================
 * Public Functions
 * ============================================================================ */

int ha_integration_init(void) {
    /* Check if HA is enabled */
    ha_enabled = cfg_getuint8("HA_ENABLED", 0);
    
    if (!ha_enabled) {
        mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, 
                "HA Integration: High availability is disabled");
        return 0;
    }
    
    /* Register callbacks with HA module */
    ha_set_become_leader_callback(on_become_leader_cb);
    ha_set_become_follower_callback(on_become_follower_cb);
    ha_set_changelog_received_callback(on_changelog_received_cb);
    ha_set_sync_complete_callback(on_sync_complete_cb);
    
    mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, 
            "HA Integration: Initialized and callbacks registered");
    
    return 0;
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
