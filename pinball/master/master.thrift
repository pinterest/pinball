// Copyright 2015, Pinterest, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Pawel Garbacki

// Definition of the token master interface.
namespace py pinball.master.thrift_lib

// A token represents an abstract item stored in the master.  Precisely
// speaking, token is not more or less than a unit of system state.  The system
// state may be queried or modified at the granularity of an individual token
// but not finer than that.
struct Token {
    // Version is a unique identifier of a token instance.  Version is modified
    // each time a token gets updated.  It is guaranteed that the same version
    // is never used twice across the lifetime of the master, including master
    // restarts.
    // Example: assuming a strictly monotonic clock, version could be the
    // timestamp of the last token modification.
    1: required i64 version;
    // Name is an arbitrary string identifying a token.  At a given point in
    // time, at most one token has a given name.  Name is not allowed to change
    // over the lifetime of a token.
    // Example: names can be structured hierarchically, e.g.,
    // /workflow/some_workflow/12345/job/waiting/some_job
    2: required string name;
    // Owner is an arbitrary string identifying the client currently owning the
    // token.  Not set, or set to an empty string if the token is not owned.
    // Master does not validate that a client is consistently using the same
    // owner string across requests.
    // Example: owner could be the name of a worker claiming tokens
    // representing tasks to execute.  As long as no two workers use the same
    // name at the same time, a worker can preserve its name across restarts.
    3: optional string owner;
    // Time, in seconds since epoch, of the ownership expiration.  The token is
    // considered owned only if owner is set to a non empty value and
    // expiration time is in the future.
    // Ownership is what makes the system robust against random client
    // failures.  If a client fails and doesn't recover, the ownership of the
    // tokens it claimed will eventually expire and they will become available
    // for other clients to claim.
    // TODO(pawel): rename this to expirationTimeSec
    // Example: expiration time can be set to infinity (i.e., max_int) if the
    // token should not be claimable.
    4: optional i64 expirationTime;
    // Priority taken into account when assigning tokens to owners or selecting
    // a subset of tokens matching a query.  A higher priority value indicates
    // a more important token.  Priorities are strict: a token with a higher 
    // priority is always preferred over a token with a lower priority.  The
    // fact that the priority is a floating point gives us the ability to
    // squeeze the priority of a new token in between the priorities of
    // existing tokens without modifying those tokens.
    // Example: a token with priority 10.5 is preferred over a token with
    // priority 10.  A token whose priority is not set (defaults to 0) is
    // preferred over a token with priority -10.
    5: optional double priority = 0;
    // Data is a blob containing application specific info stored in the token.
    // Example: in most of the cases this will be a pickled application object,
    // e.g., definition of a workflow job.
    6: optional string data;
}

// Error code identifies the reason of why an interaction with the master
// failed.
enum ErrorCode {
    // Unknown error.
    UNKNOWN = 0,
    // The version of a token in the request does not match the version stored
    // in the master.  This happens if someone else modified the token after
    // we obtained the version included in the request.
    VERSION_CONFLICT = 1,
    // Token not found.
    NOT_FOUND = 2,
    // The request is not formatted properly.
    INPUT_ERROR = 3,
}

// Exception indicating request failure.
exception TokenMasterException {
    // Error code mnemonic.
    1: ErrorCode errorCode,
    // Detailed error message.
    2: string errorMessage,
}

// Request archiving tokens.  Archived tokens are persisted in the store but
// they are removed from the master.
struct ArchiveRequest {
    1: optional list<Token> tokens;
}

// Request grouping of token names sharing a specific prefix and group suffix.
// Group suffix is a delimiter that appears after the prefix.  Group is
// defined as the sequence of charactes left of that delimiter.  If the
// delimiter does not exist, the group is the full name.  For each group we
// compute the number of tokens in that group.  Group requests can be
// used to explore the token name hierarchy.  Example:
//     token names:
//         /dir1/subdir1/token1
//         /dir1/subdir1/token2
//         /dir1/subdir2/token
//         /dir2/token
//     namePrefix:
//         /dir1/
//     groupSuffix:
//         /
//     response:
//         /dir1/subdir1/ count: 2
//         /dir1/subdir2/ count: 1
struct GroupRequest {
    // Token name prefix to be counted.
    1: optional string namePrefix;
    2: optional string groupSuffix;
}

// Counts corresponding to groups.
struct GroupResponse {
    // Mapping from group name to the number of tokens in that group.
    1: optional map<string, i32> counts;
}

// Request modifying tokens.  It supports inserting new tokens and removing or
// updating existing ones.  Modifications are atomic - either all updates and
// deletes succeed, or none does.
struct ModifyRequest {
    // List of tokens to insert or modify.  If a token on the list has version
    // set, it is treated as an update of an existing token.  Otherwise, it will
    // be inserted.
    // The request will fail if any of the tokens on this list is not present
    // in the master (or has a different version).
    1: optional list<Token> updates;
    // List of tokens to delete.  Tokens are required to have versions set.  If
    // any of them is missing in the master, the request will fail.
    2: optional list<Token> deletes;
}

// List of updated tokens.
struct ModifyResponse {
    // Tokens that have been either modified or inserted with versions set to
    // the values recorded in the master.  Deleted tokens are not included
    // here.
    1: optional list<Token> updates;
}

// Specification of tokens to retrieve.
struct Query {
    // Prefix of token names to retrieve.
    1: optional string namePrefix;
    // Maximum number of tokens to retrieve.
    // TODO(pawel): we should enforce sorting of results before truncating the
    // list.  This way we can support efficient retrieval of a token fully
    // matching the prefix.
    2: optional i32 maxTokens;
}

// Request retrieving tokens matching query specification.
struct QueryRequest {
    1: optional list<Query> queries;
}

// Tokens matching queries.
struct QueryResponse {
    // Elements on the list appear in the order of queries in the request.
    1: optional list<list<Token>> tokens;
}

// Claim ownership of tokens matching query specification.  Only tokens that are
// currently not owned will be considered.
struct QueryAndOwnRequest {
    // Owner value that will be set in the claimed tokens.
    1: optional string owner;
    // Ownership expiration time that will be set in the claimed tokens.
    2: i64 expirationTime;
    // Query specifying tokens to claim.
    3: optional Query query;
}

// Newly owned tokens.
struct QueryAndOwnResponse {
    1: optional list<Token> tokens;
}

// API exported by the master server.
service TokenMasterService {
    void archive(1: ArchiveRequest request)
        throws(1: TokenMasterException e),

    GroupResponse group(1: GroupRequest request)
        throws(1: TokenMasterException e),

    ModifyResponse modify(1: ModifyRequest request)
        throws(1: TokenMasterException e),

    QueryResponse query(1: QueryRequest request)
        throws(1: TokenMasterException e),

    QueryAndOwnResponse query_and_own(1: QueryAndOwnRequest request)
        throws(1: TokenMasterException e),
}
