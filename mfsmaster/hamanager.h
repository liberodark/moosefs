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
 */

#ifndef _HAMANAGER_H_
#define _HAMANAGER_H_

#include <stdint.h>
#include <time.h>
#include <poll.h>
#include <limits.h>

/*
 * MooseFS HA Manager
 *
 * Implements a Raft-based consensus algorithm for master server
 * high availability. Supports automatic leader election and
 * failover with real-time changelog synchronization.
 */

/* ============================================================================
 * Constants and Configuration
 * ============================================================================ */

#define HA_MAX_PEERS            16          /* Maximum number of masters in cluster */
#define HA_DEFAULT_PORT         9420        /* Default HA communication port */
#define HA_HEARTBEAT_MS         150         /* Heartbeat interval in milliseconds */
#define HA_ELECTION_TIMEOUT_MIN 1500        /* Minimum election timeout (ms) */
#define HA_ELECTION_TIMEOUT_MAX 3000        /* Maximum election timeout (ms) */
#define HA_SYNC_TIMEOUT_MS      5000        /* Sync timeout in milliseconds */
#define HA_META_DL_BLOCK        1000000     /* Metadata download block size (1MB, like metalogger) */

/* ============================================================================
 * HA State Definitions
 * ============================================================================ */

typedef enum {
    HA_STATE_INIT       = 0,    /* Initial state, not yet joined cluster */
    HA_STATE_FOLLOWER   = 1,    /* Follower - receives updates from leader */
    HA_STATE_CANDIDATE  = 2,    /* Candidate - requesting votes for election */
    HA_STATE_LEADER     = 3,    /* Leader - handles all client requests */
    HA_STATE_OBSERVER   = 4,    /* Observer - read-only, doesn't vote */
    HA_STATE_SHUTDOWN   = 5     /* Shutting down gracefully */
} ha_state_t;

/* ============================================================================
 * Message Types for HA Protocol
 * ============================================================================ */

typedef enum {
    /* Raft core messages */
    HA_MSG_REQUEST_VOTE         = 0x0001,   /* Request vote during election */
    HA_MSG_VOTE_RESPONSE        = 0x0002,   /* Response to vote request */
    HA_MSG_APPEND_ENTRIES       = 0x0003,   /* Heartbeat / log replication */
    HA_MSG_APPEND_RESPONSE      = 0x0004,   /* Response to append entries */

    /* Cluster management */
    HA_MSG_JOIN_REQUEST         = 0x0010,   /* Request to join cluster */
    HA_MSG_JOIN_RESPONSE        = 0x0011,   /* Response to join request */
    HA_MSG_LEAVE_NOTIFY         = 0x0012,   /* Graceful leave notification */
    HA_MSG_CLUSTER_CONFIG       = 0x0013,   /* Cluster configuration update */

    /* Metadata synchronization */
    HA_MSG_SYNC_REQUEST         = 0x0020,   /* Request metadata snapshot */
    HA_MSG_SYNC_RESPONSE        = 0x0021,   /* Metadata snapshot data */
    HA_MSG_CHANGELOG_ENTRY      = 0x0022,   /* Single changelog entry */
    HA_MSG_CHANGELOG_BATCH      = 0x0023,   /* Batch of changelog entries */
    HA_MSG_CHANGELOG_ACK        = 0x0024,   /* Acknowledge changelog receipt */
    HA_MSG_SYNC_INFO            = 0x0025,   /* Sync info: file size */
    HA_MSG_SYNC_CHUNK_REQUEST   = 0x0026,   /* Request chunk (offset, size) */
    HA_MSG_SYNC_CHUNK_DATA      = 0x0027,   /* Chunk data (offset, size, CRC, data) */
    HA_MSG_CATCHUP_REQUEST      = 0x0028,   /* Request changelog catchup (follower_version) */
    HA_MSG_FULL_SYNC_REQUIRED   = 0x0029,   /* Leader tells follower: need full metadata sync */

    /* Health and monitoring */
    HA_MSG_PING                 = 0x0030,   /* Simple ping */
    HA_MSG_PONG                 = 0x0031,   /* Ping response */
    HA_MSG_STATUS_REQUEST       = 0x0032,   /* Request peer status */
    HA_MSG_STATUS_RESPONSE      = 0x0033    /* Status response */
} ha_msg_type_t;

/* ============================================================================
 * Data Structures
 * ============================================================================ */

/* Peer information */
typedef struct ha_peer {
    uint32_t    id;                         /* Unique peer ID */
    uint32_t    ip;                         /* IP address */
    uint16_t    port;                       /* HA port */
    ha_state_t  state;                      /* Current state */
    uint64_t    term;                       /* Current term */
    uint64_t    last_log_index;             /* Last log index */
    uint64_t    last_log_term;              /* Term of last log entry */
    uint64_t    commit_index;               /* Committed log index */
    uint64_t    match_index;                /* Matched log index (leader only) */
    uint64_t    next_index;                 /* Next index to send (leader only) */
    double      last_heartbeat;             /* Last heartbeat time */
    int         sock;                       /* Socket descriptor */
    int         pdescpos;                   /* Position in poll descriptor array */
    uint8_t     vote_granted;               /* Vote granted in current term */
    uint8_t     is_connected;               /* Connection status */
    char        hostname[256];              /* Hostname string */
    /* Sync state for chunked transfer */
    char        sync_path[PATH_MAX];        /* Path to metadata file being synced */
    uint64_t    sync_filesize;              /* Size of file being synced */
    /* Changelog replication tracking (metalogger-style) */
    uint8_t     logstate;                   /* NONE=0, DELAYED=1, SYNC=2 */
    uint64_t    next_log_version;           /* Next changelog version to send (for DELAYED) */
    uint64_t    acked_version;              /* Last changelog version ACKed by this peer */
} ha_peer_t;

/* Peer log states (metalogger-style) */
#define HA_LOGSTATE_NONE    0               /* Not yet registered */
#define HA_LOGSTATE_DELAYED 1               /* Catching up (receiving old changelogs) */
#define HA_LOGSTATE_SYNC    2               /* In sync (receiving live changelogs) */

/* Number of changelogs to send per batch during catchup */
#define HA_CHANGELOG_BATCH_SIZE 10000

/* Changelog entry for replication */
typedef struct ha_log_entry {
    uint64_t    index;                      /* Log index */
    uint64_t    term;                       /* Term when created */
    uint64_t    meta_version;               /* Metadata version */
    uint32_t    timestamp;                  /* Unix timestamp */
    uint32_t    data_len;                   /* Data length */
    uint8_t     *data;                      /* Changelog data */
    struct ha_log_entry *next;              /* Next entry in list */
} ha_log_entry_t;

/* HA cluster state */
typedef struct ha_cluster {
    /* Identity */
    uint32_t    self_id;                    /* Our peer ID */
    uint32_t    self_ip;                    /* Our IP address */
    uint16_t    self_port;                  /* Our HA port */

    /* Raft state */
    ha_state_t  state;                      /* Current state */
    uint64_t    current_term;               /* Current term number */
    uint32_t    voted_for;                  /* Candidate voted for this term */
    uint32_t    leader_id;                  /* Current leader ID */

    /* Log state */
    uint64_t    commit_index;               /* Highest committed entry */
    uint64_t    last_applied;               /* Last applied to state machine */
    uint64_t    last_log_index;             /* Last log entry index */
    uint64_t    last_log_term;              /* Term of last log entry */

    /* Cluster membership */
    ha_peer_t   peers[HA_MAX_PEERS];        /* Peer list */
    uint32_t    peer_count;                 /* Number of peers */
    uint32_t    quorum_size;                /* Required quorum size */

    /* Election */
    uint32_t    votes_received;             /* Votes received this election */
    double      election_timeout;           /* Current election timeout */
    double      election_start;             /* Election start time */
    double      last_heartbeat_sent;        /* Last heartbeat sent (leader) */
    double      last_heartbeat_received;    /* Last heartbeat from leader */

    /* Synchronization */
    uint64_t    sync_target_version;        /* Target metadata version */
    uint8_t     sync_in_progress;           /* Sync operation in progress */

    /* Statistics */
    uint64_t    elections_started;          /* Number of elections started */
    uint64_t    elections_won;              /* Number of elections won */
    uint64_t    terms_seen;                 /* Number of terms observed */
    uint64_t    changelogs_replicated;      /* Changelogs replicated */
    uint64_t    failovers;                  /* Number of failovers */

    /* Configuration */
    uint32_t    heartbeat_interval_ms;      /* Heartbeat interval */
    uint32_t    election_timeout_min_ms;    /* Min election timeout */
    uint32_t    election_timeout_max_ms;    /* Max election timeout */
    uint8_t     enable_auto_failover;       /* Auto failover enabled */
    uint8_t     enable_read_on_follower;    /* Allow reads on follower */
} ha_cluster_t;

/* Message header */
typedef struct ha_msg_header {
    uint32_t    magic;                      /* Magic number: 0x4D465348 "MFSH" */
    uint16_t    type;                       /* Message type */
    uint16_t    version;                    /* Protocol version */
    uint32_t    sender_id;                  /* Sender peer ID */
    uint64_t    term;                       /* Sender's current term */
    uint32_t    length;                     /* Payload length */
    uint32_t    crc32;                      /* Checksum */
} __attribute__((packed)) ha_msg_header_t;

#define HA_MSG_MAGIC    0x4D465348          /* "MFSH" */
#define HA_PROTOCOL_VER 0x0001              /* Protocol version 1 */

/* RequestVote RPC */
typedef struct ha_request_vote {
    uint64_t    term;                       /* Candidate's term */
    uint32_t    candidate_id;               /* Candidate requesting vote */
    uint64_t    last_log_index;             /* Index of candidate's last log */
    uint64_t    last_log_term;              /* Term of candidate's last log */
    uint64_t    meta_version;               /* Candidate's metadata version (MooseFS) */
} __attribute__((packed)) ha_request_vote_t;

/* Vote Response */
typedef struct ha_vote_response {
    uint64_t    term;                       /* Current term for candidate */
    uint8_t     vote_granted;               /* True if vote granted */
    uint32_t    voter_id;                   /* Voter's ID */
} __attribute__((packed)) ha_vote_response_t;

/* AppendEntries RPC */
typedef struct ha_append_entries {
    uint64_t    term;                       /* Leader's term */
    uint32_t    leader_id;                  /* Leader's ID */
    uint64_t    prev_log_index;             /* Index of log entry before new */
    uint64_t    prev_log_term;              /* Term of prev_log_index entry */
    uint64_t    leader_commit;              /* Leader's commit index */
    uint32_t    entries_count;              /* Number of entries */
    /* Followed by entries_count entries */
} __attribute__((packed)) ha_append_entries_t;

/* Append Response */
typedef struct ha_append_response {
    uint64_t    term;                       /* Current term */
    uint8_t     success;                    /* True if successful */
    uint64_t    match_index;                /* Highest replicated index */
    uint32_t    follower_id;                /* Follower's ID */
} __attribute__((packed)) ha_append_response_t;

/* ============================================================================
 * Public API Functions
 * ============================================================================ */

/* Initialization and cleanup */
int ha_init(void);
void ha_term(void);
void ha_reload(void);

/* State management */
ha_state_t ha_get_state(void);
const char* ha_state_str(ha_state_t state);
int ha_is_leader(void);
int ha_is_writable(void);
uint32_t ha_get_leader_id(void);
uint64_t ha_get_current_term(void);

/* Cluster membership */
int ha_join_cluster(const char *peer_list);
int ha_leave_cluster(void);
int ha_add_peer(const char *host, uint16_t port);
int ha_remove_peer(uint32_t peer_id);
uint32_t ha_get_peer_count(void);

/* Log replication */
int ha_replicate_changelog(uint64_t version, const uint8_t *data, uint32_t len);
int ha_wait_for_commit(uint64_t index, uint32_t timeout_ms);
uint64_t ha_get_commit_index(void);

/* Synchronization */
int ha_request_full_sync(void);
int ha_request_sync(void);  /* Request full sync from leader */
int ha_is_sync_complete(void);
uint64_t ha_get_sync_progress(void);

/* Leadership */
int ha_step_down(void);
int ha_request_leadership(void);

/* Peer communication */
uint32_t ha_get_self_id(void);
uint32_t ha_get_leader_id(void);
int ha_send_to_leader(uint16_t type, const uint8_t *data, uint32_t len);
int ha_send_to_peer(uint32_t peer_id, uint16_t type, const uint8_t *data, uint32_t len);
int ha_request_catchup(uint64_t my_version);  /* Request changelog catchup from leader */

/* Monitoring and stats */
void ha_get_cluster_status(ha_cluster_t *status);
void ha_get_peer_status(uint32_t peer_id, ha_peer_t *status);
void ha_log_status(void);

/* Sync data callback - receives message type, payload and length */
typedef void (*ha_on_sync_data_fn)(uint16_t msg_type, const uint8_t *data, uint32_t len);
void ha_set_sync_data_callback(ha_on_sync_data_fn fn);

/* Callbacks for integration with master */
typedef void (*ha_on_become_leader_fn)(void);
typedef void (*ha_on_become_follower_fn)(uint32_t leader_id);
typedef void (*ha_on_changelog_received_fn)(uint64_t version, const uint8_t *data, uint32_t len);
typedef void (*ha_on_sync_complete_fn)(void);

void ha_set_become_leader_callback(ha_on_become_leader_fn fn);
void ha_set_become_follower_callback(ha_on_become_follower_fn fn);
void ha_set_changelog_received_callback(ha_on_changelog_received_fn fn);
void ha_set_sync_complete_callback(ha_on_sync_complete_fn fn);

/* Network integration */
void ha_desc(struct pollfd *pdesc, uint32_t *ndesc);
void ha_serve(struct pollfd *pdesc);
void ha_keep_alive(void);

#endif /* _HAMANAGER_H_ */
