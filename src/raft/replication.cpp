#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"
#include "storage_codec.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace kvstore::raft {
namespace {
// Bound each in-flight copy; a single maximum-size command still fits.
constexpr std::size_t kBatchEntries = 64;
constexpr std::size_t kBatchBytes = 2 * 1024 * 1024;
constexpr std::size_t kEntryHeaderBytes = 25;

bool ValidAppend(const AppendEntriesRequest& request) {
    if (request.term == 0 || request.prev_log_term > request.term ||
        ((request.prev_log_index == 0) != (request.prev_log_term == 0)) ||
        request.prev_log_index > std::numeric_limits<LogIndex>::max() - kMaxRaftEntries ||
        request.entries.size() > kMaxRaftEntries) { return false; }
    auto term = request.prev_log_term;
    std::size_t bytes = 13;  // Suffix record tag, first index, and entry count.
    for (std::size_t i = 0; i < request.entries.size(); ++i) {
        const auto& entry = request.entries[i];
        if (entry.index != request.prev_log_index + i + 1 || entry.term == 0 ||
            entry.term < term || entry.term > request.term || !detail::ValidCommand(entry.command)) { return false; }
        const auto size = kEntryHeaderBytes + entry.command.key.size() + entry.command.value.size();
        if (size > kMaxRaftRecordBytes - bytes) { return false; }
        bytes += size;
        term = entry.term;
    }
    return true;
}
}  // namespace

Proposal Node::Propose(Command command) {
    EnsureUsable();
    if (!storage_ || role_ != Role::Leader) { throw std::logic_error("Only a durable Raft leader accepts proposals"); }
    if (command.type == CommandType::NoOp || !detail::ValidCommand(command)) {
        throw std::invalid_argument("Proposal must be a valid Put or Delete command");
    }
    return AppendProposal(std::move(command));
}

Proposal Node::ProposeReadBarrier() {
    EnsureUsable();
    if (!storage_ || role_ != Role::Leader) { throw std::logic_error("Only a durable leader accepts a read barrier"); }
    return AppendProposal({CommandType::NoOp, "", ""});
}

Proposal Node::AppendProposal(Command command) {
    const Proposal proposal{status().last_log_index + 1, status().term};
    try { storage_->Append({{proposal.index, proposal.term, std::move(command)}}); }
    catch (...) {
        // Invalid input or capacity checks before a write leave Storage usable.
        if (!storage_->usable()) { Fail(); }
        throw;
    }
    try {
        AdvanceCommit();
        for (auto& peer : peers_) { SendAppend(peer); }
    } catch (...) { Fail(); throw; }
    return proposal;
}

std::vector<LogEntry> Node::CommittedEntries(LogIndex after) const {
    const auto& state = persistent_state();
    if (after > commit_index_ || after < state.base.index) { throw std::invalid_argument("Committed cursor is outside the retained log"); }
    return {state.log.begin() + static_cast<std::ptrdiff_t>(after - state.base.index),
            state.log.begin() + static_cast<std::ptrdiff_t>(commit_index_ - state.base.index)};
}

void Node::CompactLog(LogIndex index, std::uint32_t fingerprint) {
    EnsureUsable();
    if (!storage_ || index > last_applied_) { throw std::invalid_argument("Only applied entries can be truncated"); }
    try { storage_->CompactPrefix(index, fingerprint); }
    catch (...) { if (!storage_->usable()) { Fail(); } throw; }
}

std::size_t Node::live_log_bytes() const { EnsureUsable(); return storage_ ? storage_->log_bytes() : 0; }

void Node::SendAppend(Peer& peer, bool retry) {
    if (peer.pending) {
        // Retry the exact same request and ID, so delayed responses remain useful.
        if (retry) { outbox_.push_back({config_.id, peer.id, peer.pending->rpc_id, peer.pending->request}); }
        return;
    }
    if (peer.pending_snapshot) {
        if (retry) { outbox_.push_back({config_.id, peer.id, peer.pending_snapshot->rpc_id, peer.pending_snapshot->request}); }
        return;
    }
    const auto& state = persistent_state();
    if (peer.next_index <= state.base.index) {
        // The entries this peer needs are gone. Truncation always follows a durable snapshot.
        if (!snapshot_ || snapshot_->index < state.base.index) { throw std::logic_error("Truncated Raft log has no snapshot"); }
        peer.pending_snapshot = PendingSnapshot{NextRpcId(), {state.current_term, config_.id, *snapshot_}};
        outbox_.push_back({config_.id, peer.id, peer.pending_snapshot->rpc_id, peer.pending_snapshot->request});
        return;
    }
    const auto previous = peer.next_index - 1;
    AppendEntriesRequest request{state.current_term, config_.id, previous, state.TermAt(previous), {}, commit_index_};
    std::size_t bytes = 13;
    for (std::size_t offset = static_cast<std::size_t>(previous - state.base.index); offset < state.log.size(); ++offset) {
        const auto& entry = state.log[offset];
        const auto size = kEntryHeaderBytes + entry.command.key.size() + entry.command.value.size();
        if (request.entries.size() == kBatchEntries || size > kBatchBytes - bytes) { break; }
        request.entries.push_back(entry);
        bytes += size;
    }
    peer.pending = PendingAppend{NextRpcId(), std::move(request)};
    outbox_.push_back({config_.id, peer.id, peer.pending->rpc_id, peer.pending->request});
}

void Node::SendHeartbeats() {
    for (auto& peer : peers_) { SendAppend(peer, true); }
}

void Node::AdvanceCommit() {
    std::vector<LogIndex> matched;
    matched.reserve(peers_.size() + 1);
    matched.push_back(status().last_log_index);
    for (const auto& peer : peers_) { matched.push_back(peer.match_index); }
    std::sort(matched.begin(), matched.end());
    // At least majority_size() members have replicated through this position.
    const auto candidate = matched[matched.size() - majority_size()];
    // Counting replicas alone cannot commit an entry from an older term.
    if (candidate > commit_index_ && persistent_state().TermAt(candidate) == persistent_state().current_term) {
        commit_index_ = candidate;
    }
}

void Node::Handle(const Message& message, const AppendEntriesRequest& request) {
    if (request.leader_id != message.from || !ValidAppend(request)) { return; }
    ObserveHigherTerm(request.term);
    bool success = false;
    if (request.term == persistent_state().current_term) {
        role_ = Role::Follower;
        leader_id_ = request.leader_id;
        heartbeat_elapsed_ = std::chrono::milliseconds{0};
        ClearElection();
        ResetElectionTimer();
        const auto& state = persistent_state();
        // Everything through the base is committed, so a leader's older prefix must agree with it.
        const bool below_base = request.prev_log_index < state.base.index;
        const bool prefix_matches = below_base ||
            (request.prev_log_index <= state.last_index() && state.TermAt(request.prev_log_index) == request.prev_log_term);
        if (prefix_matches) {
            std::size_t different = below_base
                ? static_cast<std::size_t>(std::min<LogIndex>(request.entries.size(), state.base.index - request.prev_log_index)) : 0;
            while (different < request.entries.size()) {
                const auto& incoming = request.entries[different];
                if (incoming.index > state.last_index() || state.TermAt(incoming.index) != incoming.term) { break; }
                if (!detail::SameEntry(state.EntryAt(incoming.index), incoming)) {
                    throw std::runtime_error("Raft index and term identify different commands");
                }
                ++different;
            }
            try {
                if (different < request.entries.size()) {
                    const auto first = request.entries[different].index;
                    if (first <= commit_index_) { throw std::runtime_error("Cannot replace a committed Raft entry"); }
                    // Matching entries and an empty heartbeat never truncate a trailing suffix.
                    storage_->ReplaceSuffix(first, {request.entries.begin() + static_cast<std::ptrdiff_t>(different),
                                                   request.entries.end()});
                }
                const auto covered = request.prev_log_index + request.entries.size();
                commit_index_ = std::max(commit_index_, std::min(request.leader_commit, covered));
                success = true;  // Any changed suffix has already been synced.
            } catch (const std::length_error&) {
                // A full log is backpressure until this node truncates, not a node failure.
                if (!storage_->usable()) { throw; }
            }
        }
    }
    outbox_.push_back({config_.id, message.from, message.rpc_id,
                      AppendEntriesResponse{persistent_state().current_term, success}});
}

void Node::Handle(const Message& message, const AppendEntriesResponse& response) {
    ObserveHigherTerm(response.term);
    if (role_ != Role::Leader || response.term != persistent_state().current_term) { return; }
    auto& peer = *std::find_if(peers_.begin(), peers_.end(), [&](const Peer& value) { return value.id == message.from; });
    if (!peer.pending || peer.pending->rpc_id != message.rpc_id) { return; }
    const auto covered = peer.pending->request.prev_log_index + peer.pending->request.entries.size();
    const auto sent_commit = peer.pending->request.leader_commit;
    peer.pending.reset();
    if (!response.success) {
        if (peer.next_index > peer.match_index + 1) {
            --peer.next_index;
            SendAppend(peer);
        }  // A rejection at the known prefix waits for the next heartbeat, avoiding a busy loop.
        return;
    }
    peer.match_index = std::max(peer.match_index, covered);
    peer.next_index = peer.match_index + 1;
    peer.acknowledged_commit = std::max(peer.acknowledged_commit, std::min(sent_commit, covered));
    ContinueReplication();
}

void Node::ContinueReplication() {
    AdvanceCommit();
    for (auto& target : peers_) {
        if (target.next_index <= status().last_log_index || target.acknowledged_commit < commit_index_) {
            SendAppend(target);
        }
    }
}

void Node::Handle(const Message& message, const InstallSnapshotRequest& request) {
    const auto& snapshot = request.snapshot;
    if (request.leader_id != message.from || request.term == 0 || snapshot.index == 0 ||
        snapshot.term == 0 || snapshot.term > request.term) { return; }
    ObserveHigherTerm(request.term);
    const auto& state = persistent_state();
    const auto reply = [&](bool success) {
        outbox_.push_back({config_.id, message.from, message.rpc_id, InstallSnapshotResponse{state.current_term, success}});
    };
    if (request.term != state.current_term) { reply(false); return; }
    role_ = Role::Follower;
    leader_id_ = request.leader_id;
    heartbeat_elapsed_ = std::chrono::milliseconds{0};
    ClearElection();
    ResetElectionTimer();
    if (snapshot.index <= commit_index_) { reply(true); return; }
    if (snapshot.index <= state.last_index() && state.TermAt(snapshot.index) == snapshot.term) {
        // This log already holds the prefix, and later entries may be acknowledged: keep them.
        commit_index_ = snapshot.index;
        reply(true);
        return;
    }
    pending_install_ = PendingInstall{message.from, message.rpc_id, snapshot};
}

void Node::Handle(const Message& message, const InstallSnapshotResponse& response) {
    ObserveHigherTerm(response.term);
    if (role_ != Role::Leader || response.term != persistent_state().current_term) { return; }
    auto& peer = *std::find_if(peers_.begin(), peers_.end(), [&](const Peer& value) { return value.id == message.from; });
    if (!peer.pending_snapshot || peer.pending_snapshot->rpc_id != message.rpc_id) { return; }
    const auto installed = peer.pending_snapshot->request.snapshot.index;
    peer.pending_snapshot.reset();
    if (!response.success) { return; }  // Files are still in transit; the next heartbeat asks again.
    peer.match_index = std::max(peer.match_index, installed);
    peer.next_index = peer.match_index + 1;
    peer.acknowledged_commit = std::max(peer.acknowledged_commit, installed);
    ContinueReplication();
}

std::optional<SnapshotInfo> Node::PendingSnapshotInstall() const {
    if (!pending_install_) { return std::nullopt; }
    return pending_install_->snapshot;
}

void Node::FinishSnapshotInstall(bool installed) {
    EnsureUsable();
    if (!pending_install_) { throw std::logic_error("No snapshot install is pending"); }
    const auto pending = *pending_install_;
    pending_install_.reset();
    try {
        if (installed) {
            storage_->ResetToSnapshot(pending.snapshot);
            commit_index_ = last_applied_ = pending.snapshot.index;
            snapshot_ = pending.snapshot;
        }
        outbox_.push_back({config_.id, pending.from, pending.rpc_id,
                          InstallSnapshotResponse{persistent_state().current_term, installed}});
    } catch (...) { Fail(); throw; }
}

}  // namespace kvstore::raft
