/*
 * MooseFS Community Edition - High Availability Module
 * 
 * Copyright (C) 2025 - Open Source Community Contribution
 * 
 * This file is part of MooseFS.
 * 
 * MooseFS is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2 (only).
 * 
 * Implementation of Raft consensus algorithm for MooseFS master HA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>

#include "hamanager.h"
#include "cfg.h"
#include "main.h"
#include "sockets.h"
#include "clocks.h"
#include "datapack.h"
#include "mfslog.h"
#include "massert.h"
#include "metadata.h"
#include "changelog.h"
#include "crc.h"

/* ============================================================================
 * Internal State
 * ============================================================================ */

static ha_cluster_t *cluster = NULL;
static int ha_lsock = -1;                   /* Listening socket */
static pthread_mutex_t ha_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Callbacks */
static ha_on_become_leader_fn on_become_leader = NULL;
static ha_on_become_follower_fn on_become_follower = NULL;
static ha_on_changelog_received_fn on_changelog_received = NULL;
static ha_on_sync_complete_fn on_sync_complete = NULL;

/* Configuration */
static char *HA_PeerList = NULL;
static char *HA_BindHost = NULL;
static uint16_t HA_Port = HA_DEFAULT_PORT;
static uint8_t HA_Enabled = 0;

/* Log entries (in-memory ring buffer) - reserved for future use */
#define HA_LOG_BUFFER_SIZE 100000
static ha_log_entry_t *log_buffer __attribute__((unused)) = NULL;
static uint64_t log_buffer_start __attribute__((unused)) = 0;
static uint64_t log_buffer_count __attribute__((unused)) = 0;

/* Forward declarations */
static void ha_send_heartbeats(void);

/* ============================================================================
 * Utility Functions
 * ============================================================================ */

static double ha_random_timeout(void) {
    uint32_t range = cluster->election_timeout_max_ms - cluster->election_timeout_min_ms;
    uint32_t random_ms;
    if (range == 0) {
        random_ms = cluster->election_timeout_min_ms;
    } else {
        random_ms = cluster->election_timeout_min_ms + (rand() % range);
    }
    return (double)random_ms / 1000.0;
}

static uint32_t ha_calculate_quorum(uint32_t total) {
    return (total / 2) + 1;
}

static uint32_t ha_generate_peer_id(uint32_t ip, uint16_t port) {
    /* Simple hash combining IP and port */
    return (ip ^ ((uint32_t)port << 16) ^ ((uint32_t)port >> 16));
}

static uint32_t ha_crc32(const uint8_t *data, uint32_t len) {
    return mycrc32(0, data, len);
}

const char* ha_state_str(ha_state_t state) {
    switch (state) {
        case HA_STATE_INIT:      return "INIT";
        case HA_STATE_FOLLOWER:  return "FOLLOWER";
        case HA_STATE_CANDIDATE: return "CANDIDATE";
        case HA_STATE_LEADER:    return "LEADER";
        case HA_STATE_OBSERVER:  return "OBSERVER";
        case HA_STATE_SHUTDOWN:  return "SHUTDOWN";
        default:                 return "UNKNOWN";
    }
}

/* ============================================================================
 * Network Functions
 * ============================================================================ */

static int ha_connect_peer(ha_peer_t *peer) {
    int sock;
    int flags;
    
    if (peer->is_connected && peer->sock >= 0) {
        return 0;
    }
    
    sock = tcpsocket();
    if (sock < 0) {
        return -1;
    }
    
    tcpnodelay(sock);
    
    /* Set a connection timeout using setsockopt */
    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    
    if (tcpnumconnect(sock, peer->ip, peer->port) < 0) {
        tcpclose(sock);
        return -1;
    }
    
    /* Ensure socket is in blocking mode */
    flags = fcntl(sock, F_GETFL, 0);
    if (flags != -1) {
        fcntl(sock, F_SETFL, flags & ~O_NONBLOCK);
    }
    
    peer->sock = sock;
    peer->is_connected = 1;
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA: Connected to peer %u at %s:%u",
            peer->id, peer->hostname, peer->port);
    
    return 0;
}

static void ha_disconnect_peer(ha_peer_t *peer) {
    if (peer->sock >= 0) {
        tcpclose(peer->sock);
        peer->sock = -1;
    }
    peer->is_connected = 0;
}

static int ha_send_message(ha_peer_t *peer, uint16_t type, 
                           const uint8_t *payload, uint32_t payload_len) {
    ha_msg_header_t header;
    uint8_t *buffer;
    uint32_t total_len;
    ssize_t sent;
    int flags;
    
    if (!peer->is_connected || peer->sock < 0) {
        if (ha_connect_peer(peer) < 0) {
            return -1;
        }
    }
    
    /* Ensure socket is blocking before send */
    flags = fcntl(peer->sock, F_GETFL, 0);
    if (flags != -1 && (flags & O_NONBLOCK)) {
        fcntl(peer->sock, F_SETFL, flags & ~O_NONBLOCK);
    }
    
    /* Build header */
    header.magic = HA_MSG_MAGIC;
    header.type = type;
    header.version = HA_PROTOCOL_VER;
    header.sender_id = cluster->self_id;
    header.term = cluster->current_term;
    header.length = payload_len;
    header.crc32 = payload_len > 0 ? ha_crc32(payload, payload_len) : 0;
    
    total_len = sizeof(header) + payload_len;
    buffer = malloc(total_len);
    if (buffer == NULL) {
        return -1;
    }
    
    memcpy(buffer, &header, sizeof(header));
    if (payload_len > 0 && payload != NULL) {
        memcpy(buffer + sizeof(header), payload, payload_len);
    }
    
    sent = write(peer->sock, buffer, total_len);
    free(buffer);
    
    if (sent < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: Failed to send message to peer %u: %s (errno=%d, sock=%d)",
                peer->id, strerror(errno), errno, peer->sock);
        ha_disconnect_peer(peer);
        return -1;
    }
    
    if (sent != (ssize_t)total_len) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: Partial send to peer %u: sent %zd of %u bytes",
                peer->id, sent, total_len);
        ha_disconnect_peer(peer);
        return -1;
    }
    
    return 0;
}

static int ha_broadcast_message(uint16_t type, const uint8_t *payload, uint32_t payload_len) {
    uint32_t i;
    int success_count = 0;
    
    for (i = 0; i < cluster->peer_count; i++) {
        if (cluster->peers[i].id != cluster->self_id) {
            if (ha_send_message(&cluster->peers[i], type, payload, payload_len) == 0) {
                success_count++;
            }
        }
    }
    
    return success_count;
}

/* ============================================================================
 * Raft Core Functions
 * ============================================================================ */

static void ha_become_follower(uint64_t term, uint32_t leader_id) {
    ha_state_t old_state = cluster->state;
    
    pthread_mutex_lock(&ha_mutex);
    
    cluster->state = HA_STATE_FOLLOWER;
    cluster->current_term = term;
    cluster->voted_for = 0;
    cluster->leader_id = leader_id;
    cluster->election_timeout = ha_random_timeout();
    cluster->last_heartbeat_received = monotonic_seconds();
    
    pthread_mutex_unlock(&ha_mutex);
    
    if (old_state != HA_STATE_FOLLOWER) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
                "HA: Became FOLLOWER in term %lu, leader is peer %u",
                term, leader_id);
        
        if (on_become_follower != NULL) {
            on_become_follower(leader_id);
        }
    }
}

static void ha_become_candidate(void) {
    uint8_t payload[sizeof(ha_request_vote_t)];
    uint8_t *ptr = payload;
    
    pthread_mutex_lock(&ha_mutex);
    
    cluster->state = HA_STATE_CANDIDATE;
    cluster->current_term++;
    cluster->voted_for = cluster->self_id;
    cluster->votes_received = 1;  /* Vote for self */
    cluster->election_timeout = ha_random_timeout();
    cluster->election_start = monotonic_seconds();
    cluster->elections_started++;
    
    /* Build RequestVote message */
    put64bit(&ptr, cluster->current_term);
    put32bit(&ptr, cluster->self_id);
    put64bit(&ptr, cluster->last_log_index);
    put64bit(&ptr, cluster->last_log_term);
    
    pthread_mutex_unlock(&ha_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
            "HA: Starting election for term %lu", cluster->current_term);
    
    /* Request votes from all peers */
    int sent = ha_broadcast_message(HA_MSG_REQUEST_VOTE, payload, sizeof(payload));
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
            "HA: Sent REQUEST_VOTE to %d peers", sent);
}

static void ha_become_leader(void) {
    uint32_t i;
    
    pthread_mutex_lock(&ha_mutex);
    
    cluster->state = HA_STATE_LEADER;
    cluster->leader_id = cluster->self_id;
    cluster->elections_won++;
    
    /* Initialize leader state for each peer */
    for (i = 0; i < cluster->peer_count; i++) {
        if (cluster->peers[i].id != cluster->self_id) {
            cluster->peers[i].next_index = cluster->last_log_index + 1;
            cluster->peers[i].match_index = 0;
        }
    }
    
    pthread_mutex_unlock(&ha_mutex);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
            "HA: Became LEADER in term %lu", cluster->current_term);
    
    if (on_become_leader != NULL) {
        on_become_leader();
    }
    
    /* Send initial heartbeat to all followers */
    ha_send_heartbeats();
}

static void ha_send_heartbeats(void) {
    uint8_t payload[sizeof(ha_append_entries_t)];
    uint8_t *ptr = payload;
    uint32_t i;
    
    if (cluster->state != HA_STATE_LEADER) {
        return;
    }
    
    pthread_mutex_lock(&ha_mutex);
    
    /* Build AppendEntries (heartbeat) message */
    put64bit(&ptr, cluster->current_term);
    put32bit(&ptr, cluster->self_id);
    put64bit(&ptr, cluster->last_log_index);
    put64bit(&ptr, cluster->last_log_term);
    put64bit(&ptr, cluster->commit_index);
    put32bit(&ptr, 0);  /* No entries for heartbeat */
    
    cluster->last_heartbeat_sent = monotonic_seconds();
    
    pthread_mutex_unlock(&ha_mutex);
    
    /* Send to all peers */
    for (i = 0; i < cluster->peer_count; i++) {
        if (cluster->peers[i].id != cluster->self_id) {
            ha_send_message(&cluster->peers[i], HA_MSG_APPEND_ENTRIES, 
                          payload, sizeof(payload));
        }
    }
}

/* ============================================================================
 * Message Handlers
 * ============================================================================ */

static void ha_handle_request_vote(ha_peer_t *peer, const uint8_t *data, uint32_t len) {
    uint8_t response[sizeof(ha_vote_response_t)];
    uint8_t *ptr = response;
    const uint8_t *rptr = data;
    uint64_t term, last_log_index, last_log_term;
    uint32_t candidate_id;
    uint8_t vote_granted = 0;
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
            "HA: Received REQUEST_VOTE from peer %u, len=%u", peer->id, len);
    
    if (len < sizeof(ha_request_vote_t)) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: REQUEST_VOTE too short: %u < %zu", len, sizeof(ha_request_vote_t));
        return;
    }
    
    term = get64bit(&rptr);
    candidate_id = get32bit(&rptr);
    last_log_index = get64bit(&rptr);
    last_log_term = get64bit(&rptr);
    
    pthread_mutex_lock(&ha_mutex);
    
    /* Rule 1: Reply false if term < currentTerm */
    if (term < cluster->current_term) {
        vote_granted = 0;
    }
    /* Rule 2: If term > currentTerm, become follower */
    else if (term > cluster->current_term) {
        cluster->current_term = term;
        cluster->state = HA_STATE_FOLLOWER;
        cluster->voted_for = 0;
        cluster->leader_id = 0;
    }
    
    /* Grant vote if we haven't voted yet and candidate's log is up-to-date */
    if (term >= cluster->current_term &&
        (cluster->voted_for == 0 || cluster->voted_for == candidate_id)) {
        
        /* Check if candidate's log is at least as up-to-date as ours */
        if (last_log_term > cluster->last_log_term ||
            (last_log_term == cluster->last_log_term && 
             last_log_index >= cluster->last_log_index)) {
            
            vote_granted = 1;
            cluster->voted_for = candidate_id;
            cluster->last_heartbeat_received = monotonic_seconds();
            
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                    "HA: Granting vote to candidate %u for term %lu",
                    candidate_id, term);
        }
    }
    
    /* Build response */
    put64bit(&ptr, cluster->current_term);
    put8bit(&ptr, vote_granted);
    put32bit(&ptr, cluster->self_id);
    
    pthread_mutex_unlock(&ha_mutex);
    
    ha_send_message(peer, HA_MSG_VOTE_RESPONSE, response, sizeof(response));
}

static void ha_handle_vote_response(ha_peer_t *peer, const uint8_t *data, uint32_t len) {
    const uint8_t *rptr = data;
    uint64_t term;
    uint8_t vote_granted;
    uint32_t voter_id;
    
    (void)peer;  /* Used for future extensions */
    
    if (len < sizeof(ha_vote_response_t)) {
        return;
    }
    
    term = get64bit(&rptr);
    vote_granted = get8bit(&rptr);
    voter_id = get32bit(&rptr);
    
    pthread_mutex_lock(&ha_mutex);
    
    /* Ignore if not candidate */
    if (cluster->state != HA_STATE_CANDIDATE) {
        pthread_mutex_unlock(&ha_mutex);
        return;
    }
    
    /* If term is higher, become follower */
    if (term > cluster->current_term) {
        pthread_mutex_unlock(&ha_mutex);
        ha_become_follower(term, 0);
        return;
    }
    
    /* Count vote if for current term */
    if (term == cluster->current_term && vote_granted) {
        cluster->votes_received++;
        
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                "HA: Received vote from peer %u (%u/%u votes)",
                voter_id, cluster->votes_received, cluster->quorum_size);
        
        /* Check if we have quorum */
        if (cluster->votes_received >= cluster->quorum_size) {
            pthread_mutex_unlock(&ha_mutex);
            ha_become_leader();
            return;
        }
    }
    
    pthread_mutex_unlock(&ha_mutex);
}

static void ha_handle_append_entries(ha_peer_t *peer, const uint8_t *data, uint32_t len) {
    uint8_t response[sizeof(ha_append_response_t)];
    uint8_t *ptr = response;
    const uint8_t *rptr = data;
    uint64_t term, prev_log_index, prev_log_term, leader_commit;
    uint32_t leader_id, entries_count;
    uint8_t success = 0;
    
    if (len < sizeof(ha_append_entries_t)) {
        return;
    }
    
    term = get64bit(&rptr);
    leader_id = get32bit(&rptr);
    prev_log_index = get64bit(&rptr);
    prev_log_term = get64bit(&rptr);
    leader_commit = get64bit(&rptr);
    entries_count = get32bit(&rptr);
    
    (void)prev_log_term;  /* TODO: Use for log consistency check */
    
    pthread_mutex_lock(&ha_mutex);
    
    /* Rule 1: Reply false if term < currentTerm */
    if (term < cluster->current_term) {
        success = 0;
    } else {
        /* Update term if necessary */
        if (term > cluster->current_term) {
            cluster->current_term = term;
            cluster->voted_for = 0;
        }
        
        /* Recognize leader and reset election timeout */
        cluster->leader_id = leader_id;
        cluster->last_heartbeat_received = monotonic_seconds();
        
        if (cluster->state != HA_STATE_FOLLOWER) {
            pthread_mutex_unlock(&ha_mutex);
            ha_become_follower(term, leader_id);
            pthread_mutex_lock(&ha_mutex);
        }
        
        /* Log consistency check */
        if (prev_log_index == 0 || 
            (prev_log_index <= cluster->last_log_index /* && check term match */)) {
            success = 1;
            
            /* Apply entries if any */
            if (entries_count > 0) {
                /* Process entries - would need to handle actual log entries here */
                /* For now, just acknowledge */
            }
            
            /* Update commit index */
            if (leader_commit > cluster->commit_index) {
                cluster->commit_index = 
                    (leader_commit < cluster->last_log_index) ? 
                    leader_commit : cluster->last_log_index;
            }
        }
    }
    
    /* Build response */
    put64bit(&ptr, cluster->current_term);
    put8bit(&ptr, success);
    put64bit(&ptr, cluster->last_log_index);
    put32bit(&ptr, cluster->self_id);
    
    pthread_mutex_unlock(&ha_mutex);
    
    ha_send_message(peer, HA_MSG_APPEND_RESPONSE, response, sizeof(response));
}

static void ha_handle_append_response(ha_peer_t *peer, const uint8_t *data, uint32_t len) {
    const uint8_t *rptr = data;
    uint64_t term, match_index;
    uint8_t success;
    uint32_t follower_id;
    
    if (len < sizeof(ha_append_response_t)) {
        return;
    }
    
    term = get64bit(&rptr);
    success = get8bit(&rptr);
    match_index = get64bit(&rptr);
    follower_id = get32bit(&rptr);
    
    (void)follower_id;  /* Used peer instead */
    
    pthread_mutex_lock(&ha_mutex);
    
    /* Ignore if not leader */
    if (cluster->state != HA_STATE_LEADER) {
        pthread_mutex_unlock(&ha_mutex);
        return;
    }
    
    /* Step down if higher term */
    if (term > cluster->current_term) {
        pthread_mutex_unlock(&ha_mutex);
        ha_become_follower(term, 0);
        return;
    }
    
    /* Update peer state */
    if (success) {
        peer->match_index = match_index;
        peer->next_index = match_index + 1;
        
        /* Check if we can advance commit index */
        /* Would need to implement proper commit index advancement */
    } else {
        /* Decrement next_index and retry */
        if (peer->next_index > 1) {
            peer->next_index--;
        }
    }
    
    pthread_mutex_unlock(&ha_mutex);
}

static void ha_handle_changelog_entry(ha_peer_t *peer, const uint8_t *data, uint32_t len) {
    const uint8_t *rptr = data;
    uint64_t version;
    uint32_t data_len;
    
    if (len < 12) {  /* version(8) + data_len(4) */
        return;
    }
    
    version = get64bit(&rptr);
    data_len = get32bit(&rptr);
    
    if (len < 12 + data_len) {
        return;
    }
    
    /* Apply changelog if we're a follower */
    if (cluster->state == HA_STATE_FOLLOWER && on_changelog_received != NULL) {
        on_changelog_received(version, rptr, data_len);
    }
    
    /* Send acknowledgment */
    uint8_t ack[8];
    uint8_t *ptr = ack;
    put64bit(&ptr, version);
    ha_send_message(peer, HA_MSG_CHANGELOG_ACK, ack, sizeof(ack));
}

/* ============================================================================
 * Main Message Dispatcher
 * ============================================================================ */

static void ha_handle_message(ha_peer_t *peer, const uint8_t *data, uint32_t len) {
    ha_msg_header_t header;
    const uint8_t *payload;
    uint32_t payload_len;
    
    if (len < sizeof(header)) {
        return;
    }
    
    memcpy(&header, data, sizeof(header));
    
    /* Validate header */
    if (header.magic != HA_MSG_MAGIC) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: Invalid magic number 0x%08X from peer %u (expected 0x%08X)", 
                header.magic, peer->id, HA_MSG_MAGIC);
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
            "HA: Received message type=%u from peer %u, term=%lu, len=%u",
            header.type, peer->id, header.term, header.length);
    
    if (header.version != HA_PROTOCOL_VER) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: Protocol version mismatch from peer %u", peer->id);
        return;
    }
    
    payload = data + sizeof(header);
    payload_len = header.length;
    
    /* Verify CRC if payload present */
    if (payload_len > 0) {
        if (ha_crc32(payload, payload_len) != header.crc32) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                    "HA: CRC mismatch from peer %u", peer->id);
            return;
        }
    }
    
    /* Update peer's term if higher */
    if (header.term > cluster->current_term) {
        ha_become_follower(header.term, 0);
    }
    
    /* Dispatch based on message type */
    switch (header.type) {
        case HA_MSG_REQUEST_VOTE:
            ha_handle_request_vote(peer, payload, payload_len);
            break;
        case HA_MSG_VOTE_RESPONSE:
            ha_handle_vote_response(peer, payload, payload_len);
            break;
        case HA_MSG_APPEND_ENTRIES:
            ha_handle_append_entries(peer, payload, payload_len);
            break;
        case HA_MSG_APPEND_RESPONSE:
            ha_handle_append_response(peer, payload, payload_len);
            break;
        case HA_MSG_CHANGELOG_ENTRY:
            ha_handle_changelog_entry(peer, payload, payload_len);
            break;
        case HA_MSG_PING:
            ha_send_message(peer, HA_MSG_PONG, NULL, 0);
            break;
        case HA_MSG_PONG:
            peer->last_heartbeat = monotonic_seconds();
            break;
        default:
            mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                    "HA: Unknown message type %u from peer %u", 
                    header.type, peer->id);
    }
}

/* ============================================================================
 * Timer Functions
 * ============================================================================ */

static void ha_check_election_timeout(void) {
    double now = monotonic_seconds();
    double elapsed;
    static int log_counter = 0;
    
    if (cluster == NULL || !HA_Enabled) {
        return;
    }
    
    /* Log every 10 calls (~10 seconds) to confirm function is being called */
    if (++log_counter >= 10) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
                "HA: check_timeout: state=%d, elapsed=%.2fs, timeout=%.2fs",
                cluster->state, now - cluster->last_heartbeat_received, cluster->election_timeout);
        log_counter = 0;
    }
    
    if (cluster->state == HA_STATE_LEADER) {
        /* Leader: send periodic heartbeats */
        elapsed = now - cluster->last_heartbeat_sent;
        if (elapsed >= (double)cluster->heartbeat_interval_ms / 1000.0) {
            ha_send_heartbeats();
        }
    } else if (cluster->state == HA_STATE_FOLLOWER || 
               cluster->state == HA_STATE_CANDIDATE) {
        /* Follower/Candidate: check for election timeout */
        elapsed = now - cluster->last_heartbeat_received;
        if (elapsed >= cluster->election_timeout) {
            mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, 
                    "HA: Election timeout (%.2fs elapsed, timeout=%.2fs), starting election",
                    elapsed, cluster->election_timeout);
            ha_become_candidate();
        }
    } else {
        /* Log unexpected state */
        mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, 
                "HA: check_election_timeout called in state %d", cluster->state);
    }
}

/* ============================================================================
 * Public API Implementation
 * ============================================================================ */

int ha_init(void) {
    uint32_t bindip;
    
    /* Check if HA is enabled */
    HA_Enabled = cfg_getuint8("HA_ENABLED", 0);
    if (!HA_Enabled) {
        mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, 
                "HA: High availability is disabled");
        return 0;
    }
    
    /* Load configuration */
    HA_BindHost = cfg_getstr("HA_BIND_HOST", "*");
    HA_Port = cfg_getuint16("HA_PORT", HA_DEFAULT_PORT);
    HA_PeerList = cfg_getstr("HA_PEERS", "");
    
    /* Allocate cluster state */
    cluster = calloc(1, sizeof(ha_cluster_t));
    if (cluster == NULL) {
        mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_ERR, 
                "HA: Failed to allocate cluster state");
        return -1;
    }
    
    /* Initialize cluster state */
    cluster->state = HA_STATE_INIT;
    cluster->current_term = 0;
    cluster->voted_for = 0;
    cluster->leader_id = 0;
    cluster->commit_index = 0;
    cluster->last_applied = 0;
    cluster->last_log_index = 0;
    cluster->last_log_term = 0;
    cluster->peer_count = 0;
    cluster->quorum_size = 1;
    cluster->votes_received = 0;
    cluster->heartbeat_interval_ms = cfg_getuint32("HA_HEARTBEAT_MS", HA_HEARTBEAT_MS);
    cluster->election_timeout_min_ms = cfg_getuint32("HA_ELECTION_TIMEOUT_MIN_MS", HA_ELECTION_TIMEOUT_MIN);
    cluster->election_timeout_max_ms = cfg_getuint32("HA_ELECTION_TIMEOUT_MAX_MS", HA_ELECTION_TIMEOUT_MAX);
    cluster->election_timeout = ha_random_timeout();  /* Must be after timeout_min/max init */
    cluster->enable_auto_failover = 1;
    cluster->last_heartbeat_received = monotonic_seconds();
    
    mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, 
            "HA: Timeouts configured: heartbeat=%ums, election=%u-%ums",
            cluster->heartbeat_interval_ms, 
            cluster->election_timeout_min_ms, 
            cluster->election_timeout_max_ms);
    
    /* Create listening socket */
    ha_lsock = tcpsocket();
    if (ha_lsock < 0) {
        mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR, MFSLOG_ERR, 
                "HA: Can't create socket");
        free(cluster);
        cluster = NULL;
        return -1;
    }
    
    tcpnonblock(ha_lsock);
    tcpnodelay(ha_lsock);
    tcpreuseaddr(ha_lsock);
    
    if (tcpresolve(HA_BindHost, NULL, &bindip, NULL, 1) < 0) {
        bindip = 0;
    }
    
    if (tcpnumlisten(ha_lsock, bindip, HA_Port, 100) < 0) {
        mfs_log(MFSLOG_ERRNO_SYSLOG_STDERR, MFSLOG_ERR, 
                "HA: Can't listen on %s:%u", HA_BindHost, HA_Port);
        tcpclose(ha_lsock);
        free(cluster);
        cluster = NULL;
        return -1;
    }
    
    /* Get our own IP for self identification */
    cluster->self_port = HA_Port;
    tcpgetmyaddr(ha_lsock, &cluster->self_ip, NULL);
    
    /* Determine self_id - priority: HA_NODE_ID > MATOCS_LISTEN_HOST > auto-detect */
    cluster->self_id = cfg_getuint32("HA_NODE_ID", 0);
    if (cluster->self_id == 0) {
        /* Try to get IP from MATOCS_LISTEN_HOST */
        char *matocs_host = cfg_getstr("MATOCS_LISTEN_HOST", NULL);
        if (matocs_host != NULL && matocs_host[0] != '*' && matocs_host[0] != '\0') {
            uint32_t matocs_ip;
            if (tcpresolve(matocs_host, NULL, &matocs_ip, NULL, 1) >= 0) {
                cluster->self_ip = matocs_ip;
            }
            free(matocs_host);
        }
        cluster->self_id = ha_generate_peer_id(cluster->self_ip, HA_Port);
    }
    
    mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_INFO, 
            "HA: Initialized, listening on %s:%u, self_id=%u",
            HA_BindHost, HA_Port, cluster->self_id);
    
    /* Parse and connect to peers */
    if (HA_PeerList != NULL && strlen(HA_PeerList) > 0) {
        ha_join_cluster(HA_PeerList);
    }
    
    /* Start as follower */
    cluster->state = HA_STATE_FOLLOWER;
    
    mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_NOTICE, 
            "HA: Starting as FOLLOWER, election_timeout=%.3fs", 
            cluster->election_timeout);
    
    /* Register with main event loop */
    main_poll_register(ha_desc, ha_serve);
    main_time_register(1, 0, ha_check_election_timeout);  /* Every 1 second */
    main_reload_register(ha_reload);
    main_destruct_register(ha_term);
    
    mfs_log(MFSLOG_SYSLOG_STDERR, MFSLOG_NOTICE, 
            "HA: Registered with main loop, will check timeout every 1s");
    
    return 0;
}

void ha_term(void) {
    uint32_t i;
    
    if (!HA_Enabled || cluster == NULL) {
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA: Shutting down");
    
    cluster->state = HA_STATE_SHUTDOWN;
    
    /* Disconnect all peers */
    for (i = 0; i < cluster->peer_count; i++) {
        ha_disconnect_peer(&cluster->peers[i]);
    }
    
    /* Close listening socket */
    if (ha_lsock >= 0) {
        tcpclose(ha_lsock);
        ha_lsock = -1;
    }
    
    /* Free configuration strings */
    if (HA_BindHost) free(HA_BindHost);
    if (HA_PeerList) free(HA_PeerList);
    
    /* Free cluster state */
    free(cluster);
    cluster = NULL;
}

void ha_reload(void) {
    if (!HA_Enabled) {
        return;
    }
    
    /* Reload configuration */
    cluster->heartbeat_interval_ms = cfg_getuint32("HA_HEARTBEAT_MS", HA_HEARTBEAT_MS);
    cluster->election_timeout_min_ms = cfg_getuint32("HA_ELECTION_TIMEOUT_MIN_MS", HA_ELECTION_TIMEOUT_MIN);
    cluster->election_timeout_max_ms = cfg_getuint32("HA_ELECTION_TIMEOUT_MAX_MS", HA_ELECTION_TIMEOUT_MAX);
    cluster->enable_auto_failover = cfg_getuint8("HA_AUTO_FAILOVER", 1);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA: Configuration reloaded");
}

ha_state_t ha_get_state(void) {
    if (cluster == NULL) {
        return HA_STATE_INIT;
    }
    return cluster->state;
}

int ha_is_leader(void) {
    return (cluster != NULL && cluster->state == HA_STATE_LEADER);
}

int ha_is_writable(void) {
    if (cluster == NULL) {
        return 1;  /* No HA, always writable */
    }
    return (cluster->state == HA_STATE_LEADER);
}

uint32_t ha_get_leader_id(void) {
    if (cluster == NULL) {
        return 0;
    }
    return cluster->leader_id;
}

uint64_t ha_get_current_term(void) {
    if (cluster == NULL) {
        return 0;
    }
    return cluster->current_term;
}

int ha_join_cluster(const char *peer_list) {
    char *list_copy, *token, *saveptr;
    char *host;
    uint16_t port;
    
    if (peer_list == NULL || strlen(peer_list) == 0) {
        return 0;
    }
    
    list_copy = strdup(peer_list);
    if (list_copy == NULL) {
        return -1;
    }
    
    token = strtok_r(list_copy, ",;", &saveptr);
    while (token != NULL) {
        /* Parse host:port */
        host = token;
        port = HA_Port;
        
        char *colon = strchr(token, ':');
        if (colon != NULL) {
            *colon = '\0';
            port = (uint16_t)atoi(colon + 1);
        }
        
        /* Skip whitespace */
        while (*host == ' ') host++;
        
        if (strlen(host) > 0) {
            ha_add_peer(host, port);
        }
        
        token = strtok_r(NULL, ",;", &saveptr);
    }
    
    free(list_copy);
    
    /* Update quorum size */
    cluster->quorum_size = ha_calculate_quorum(cluster->peer_count + 1);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
            "HA: Joined cluster with %u peers, quorum=%u",
            cluster->peer_count, cluster->quorum_size);
    
    return 0;
}

int ha_add_peer(const char *host, uint16_t port) {
    ha_peer_t *peer;
    uint32_t ip;
    
    if (cluster->peer_count >= HA_MAX_PEERS) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: Maximum peer count reached");
        return -1;
    }
    
    if (tcpresolve(host, NULL, &ip, NULL, 0) < 0) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                "HA: Failed to resolve peer %s", host);
        return -1;
    }
    
    peer = &cluster->peers[cluster->peer_count];
    memset(peer, 0, sizeof(ha_peer_t));
    
    peer->id = ha_generate_peer_id(ip, port);
    peer->ip = ip;
    peer->port = port;
    peer->sock = -1;
    peer->is_connected = 0;
    strncpy(peer->hostname, host, sizeof(peer->hostname) - 1);
    
    cluster->peer_count++;
    cluster->quorum_size = ha_calculate_quorum(cluster->peer_count + 1);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
            "HA: Added peer %u (%s:%u)", peer->id, host, port);
    
    /* Try to connect */
    ha_connect_peer(peer);
    
    return 0;
}

int ha_replicate_changelog(uint64_t version, const uint8_t *data, uint32_t len) {
    uint8_t *payload;
    uint8_t *ptr;
    uint32_t payload_len;
    int result;
    
    if (!HA_Enabled || cluster == NULL) {
        return 0;
    }
    
    if (cluster->state != HA_STATE_LEADER) {
        return -1;  /* Only leader can replicate */
    }
    
    /* Build changelog entry message */
    payload_len = 8 + 4 + len;  /* version + data_len + data */
    payload = malloc(payload_len);
    if (payload == NULL) {
        return -1;
    }
    
    ptr = payload;
    put64bit(&ptr, version);
    put32bit(&ptr, len);
    memcpy(ptr, data, len);
    
    /* Broadcast to all followers */
    result = ha_broadcast_message(HA_MSG_CHANGELOG_ENTRY, payload, payload_len);
    
    free(payload);
    
    cluster->changelogs_replicated++;
    
    return result > 0 ? 0 : -1;
}

void ha_set_become_leader_callback(ha_on_become_leader_fn fn) {
    on_become_leader = fn;
}

void ha_set_become_follower_callback(ha_on_become_follower_fn fn) {
    on_become_follower = fn;
}

void ha_set_changelog_received_callback(ha_on_changelog_received_fn fn) {
    on_changelog_received = fn;
}

void ha_set_sync_complete_callback(ha_on_sync_complete_fn fn) {
    on_sync_complete = fn;
}

/* ============================================================================
 * Network Event Handlers (for main event loop)
 * ============================================================================ */

static int ha_lsock_pdescpos = -1;

void ha_desc(struct pollfd *pdesc, uint32_t *ndesc) {
    uint32_t pos = *ndesc;
    uint32_t i;
    
    if (!HA_Enabled || cluster == NULL) {
        ha_lsock_pdescpos = -1;
        return;
    }
    
    /* Add listening socket */
    if (ha_lsock >= 0) {
        pdesc[pos].fd = ha_lsock;
        pdesc[pos].events = POLLIN;
        ha_lsock_pdescpos = pos;
        pos++;
    } else {
        ha_lsock_pdescpos = -1;
    }
    
    /* Add peer sockets */
    for (i = 0; i < cluster->peer_count; i++) {
        if (cluster->peers[i].sock >= 0 && cluster->peers[i].is_connected) {
            pdesc[pos].fd = cluster->peers[i].sock;
            pdesc[pos].events = POLLIN;
            cluster->peers[i].pdescpos = pos;
            pos++;
        } else {
            cluster->peers[i].pdescpos = -1;
        }
    }
    
    *ndesc = pos;
}

void ha_serve(struct pollfd *pdesc) {
    uint32_t i;
    int ns;
    uint8_t buffer[65536];
    ssize_t received;
    
    if (!HA_Enabled || cluster == NULL) {
        return;
    }
    
    /* Check listening socket for new connections */
    if (ha_lsock >= 0 && ha_lsock_pdescpos >= 0 && (pdesc[ha_lsock_pdescpos].revents & POLLIN)) {
        ns = tcpaccept(ha_lsock);
        if (ns >= 0) {
            tcpnonblock(ns);
            tcpnodelay(ns);
            
            /* Find peer by IP or add new one */
            uint32_t peerip;
            tcpgetpeer(ns, &peerip, NULL);
            
            mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                    "HA: New connection from %u.%u.%u.%u",
                    (peerip >> 24) & 0xFF, (peerip >> 16) & 0xFF,
                    (peerip >> 8) & 0xFF, peerip & 0xFF);
            
            /* Find matching peer */
            for (i = 0; i < cluster->peer_count; i++) {
                if (cluster->peers[i].ip == peerip) {
                    if (cluster->peers[i].sock >= 0) {
                        tcpclose(cluster->peers[i].sock);
                    }
                    cluster->peers[i].sock = ns;
                    cluster->peers[i].is_connected = 1;
                    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                            "HA: Accepted connection from peer %u", cluster->peers[i].id);
                    break;
                }
            }
            
            if (i >= cluster->peer_count) {
                /* Unknown peer, close connection */
                mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                        "HA: Unknown peer IP, closing connection");
                tcpclose(ns);
            }
        }
    }
    
    /* Process peer sockets */
    for (i = 0; i < cluster->peer_count; i++) {
        if (cluster->peers[i].pdescpos >= 0 && 
            cluster->peers[i].sock >= 0 &&
            (pdesc[cluster->peers[i].pdescpos].revents & POLLIN)) {
            
            received = read(cluster->peers[i].sock, buffer, sizeof(buffer));
            if (received > 0) {
                mfs_log(MFSLOG_SYSLOG, MFSLOG_DEBUG, 
                        "HA: Received %zd bytes from peer %u", received, cluster->peers[i].id);
                ha_handle_message(&cluster->peers[i], buffer, received);
            } else if (received == 0) {
                /* Connection closed */
                mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                        "HA: Peer %u disconnected", cluster->peers[i].id);
                ha_disconnect_peer(&cluster->peers[i]);
            } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                /* Error */
                mfs_log(MFSLOG_SYSLOG, MFSLOG_WARNING, 
                        "HA: Error reading from peer %u: %s",
                        cluster->peers[i].id, strerror(errno));
                ha_disconnect_peer(&cluster->peers[i]);
            }
        }
    }
}

void ha_keep_alive(void) {
    uint32_t i;
    double now;
    
    if (!HA_Enabled || cluster == NULL) {
        return;
    }
    
    now = monotonic_seconds();
    (void)now;  /* TODO: Use for connection timeout checks */
    
    /* Try to reconnect disconnected peers */
    for (i = 0; i < cluster->peer_count; i++) {
        if (!cluster->peers[i].is_connected) {
            ha_connect_peer(&cluster->peers[i]);
        }
    }
}

void ha_log_status(void) {
    uint32_t i;
    
    if (!HA_Enabled || cluster == NULL) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, "HA: Disabled");
        return;
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
            "HA: State=%s, Term=%lu, Leader=%u, CommitIndex=%lu",
            ha_state_str(cluster->state),
            cluster->current_term,
            cluster->leader_id,
            cluster->commit_index);
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
            "HA: Elections: started=%lu, won=%lu, Changelogs replicated=%lu",
            cluster->elections_started,
            cluster->elections_won,
            cluster->changelogs_replicated);
    
    for (i = 0; i < cluster->peer_count; i++) {
        mfs_log(MFSLOG_SYSLOG, MFSLOG_INFO, 
                "HA: Peer %u (%s:%u): connected=%u, matchIndex=%lu",
                cluster->peers[i].id,
                cluster->peers[i].hostname,
                cluster->peers[i].port,
                cluster->peers[i].is_connected,
                cluster->peers[i].match_index);
    }
}

void ha_get_cluster_status(ha_cluster_t *status) {
    if (cluster != NULL) {
        pthread_mutex_lock(&ha_mutex);
        memcpy(status, cluster, sizeof(ha_cluster_t));
        pthread_mutex_unlock(&ha_mutex);
    }
}

int ha_is_sync_complete(void) {
    if (!HA_Enabled || cluster == NULL) {
        return 1;  /* If HA disabled, consider synced */
    }
    
    pthread_mutex_lock(&ha_mutex);
    
    /* Leader is always synced */
    if (cluster->state == HA_STATE_LEADER) {
        pthread_mutex_unlock(&ha_mutex);
        return 1;
    }
    
    /* Follower is synced if commit_index matches leader's */
    int synced = (cluster->last_applied >= cluster->commit_index);
    
    pthread_mutex_unlock(&ha_mutex);
    return synced;
}

int ha_step_down(void) {
    if (!HA_Enabled || cluster == NULL) {
        return -1;
    }
    
    pthread_mutex_lock(&ha_mutex);
    
    if (cluster->state != HA_STATE_LEADER) {
        pthread_mutex_unlock(&ha_mutex);
        return -1;  /* Not leader, cannot step down */
    }
    
    mfs_log(MFSLOG_SYSLOG, MFSLOG_NOTICE, "HA: Stepping down from leadership");
    
    pthread_mutex_unlock(&ha_mutex);
    
    /* Become follower with no known leader */
    ha_become_follower(cluster->current_term, 0);
    
    return 0;
}
