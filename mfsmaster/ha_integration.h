/*
 * MooseFS Community Edition - HA Integration Module
 * 
 * Copyright (C) 2025 - Open Source Community Contribution
 * 
 * This file provides integration between the HA module and the
 * existing MooseFS master server code.
 */

#ifndef _HA_INTEGRATION_H_
#define _HA_INTEGRATION_H_

#include <stdint.h>

/*
 * Initialize HA integration
 * Called after ha_init() to set up callbacks and integration points
 */
int ha_integration_init(void);

/*
 * Terminate HA integration
 * Cleanup resources
 */
void ha_integration_term(void);

/*
 * Pre-metadata synchronization
 * Called BEFORE meta_init() to ensure metadata is available from peers
 * Returns 0 on success (metadata ready), -1 on failure
 */
int ha_pre_metadata_sync(void);

/*
 * Check if HA is enabled
 */
int ha_is_enabled(void);

/*
 * Check if this node is the leader and can accept writes
 * Returns: 1 if leader, 0 if follower/candidate
 */
int ha_can_accept_write(void);

/*
 * Check if we should replicate changelogs
 * Returns: 1 if we're leader and have followers, 0 otherwise
 */
int ha_should_replicate(void);
int ha_is_follower(void);

/*
 * Replicate a changelog entry to followers
 * version: metadata version
 * data: changelog entry data
 * len: data length
 */
int ha_replicate_changelog_entry(uint64_t version, const uint8_t *data, uint32_t len);

/*
 * Notify HA module that metadata version has changed
 */
void ha_metadata_version_changed(uint64_t version);

/*
 * Get the IP address of the current leader
 * Returns: IP address in network byte order, or 0 if unknown
 */
uint32_t ha_get_leader_ip(void);

/*
 * Get the port of the current leader
 */
uint16_t ha_get_leader_port(void);

/*
 * Wait for metadata to be synchronized with leader
 * timeout_ms: maximum time to wait
 * Returns: 0 on success, -1 on timeout
 */
int ha_wait_for_sync(uint32_t timeout_ms);

/*
 * Check if we're synchronized with the cluster
 */
int ha_is_synchronized(void);

/*
 * Request to become leader (for administrative purposes)
 */
int ha_request_leadership(void);

/*
 * Step down from leadership
 */
int ha_step_down_leadership(void);

/*
 * Get HA status for CGI/CLI
 */
typedef struct {
    uint8_t enabled;
    uint8_t state;          /* 0=init, 1=follower, 2=candidate, 3=leader */
    uint64_t term;
    uint32_t leader_id;
    uint32_t leader_ip;
    uint64_t commit_index;
    uint64_t last_applied;
    uint32_t peer_count;
    uint32_t connected_peers;
} ha_status_info_t;

void ha_get_status_info(ha_status_info_t *info);

/*
 * Format HA status as string for logging
 */
const char* ha_format_status(void);

#endif /* _HA_INTEGRATION_H_ */
