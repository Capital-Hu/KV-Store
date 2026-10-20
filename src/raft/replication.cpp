#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"
#include "storage_codec.h"

#include <algorithm>
#include <array>
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
        request.prev_log_index > kMaxRaftEntries ||
        request.entries.size() > kMaxRaftEntries - request.prev_log_index) { return false; }
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
        for (auto& peer : peers_) { SendAppend(peer); }
    } catch (...) { Fail(); throw; }
    return proposal;
}

std::vector<LogEntry> Node::CommittedEntries(LogIndex after) const {
    const auto& log = persistent_state().log;
    if (after > commit_index_) { throw std::invalid_argument("Committed cursor exceeds local commit index"); }
    return {log.begin() + static_cast<std::ptrdiff_t>(after),
            log.begin() + static_cast<std::ptrdiff_t>(commit_index_)};
}

void Node::SendAppend(Peer& peer, bool retry) {
    if (peer.pending) {
        // Retry the exact same request and ID, so delayed responses remain useful.
        if (retry) { outbox_.push_back({config_.id, peer.id, peer.pending->rpc_id, peer.pending->request}); }
        return;
    }
    const auto& state = persistent_state();
    const auto previous = peer.next_index - 1;
    AppendEntriesRequest request{state.current_term, config_.id, previous,
        previous == 0 ? 0 : state.log[previous - 1].term, {}, commit_index_};
    std::size_t bytes = 13;
    for (std::size_t offset = static_cast<std::size_t>(previous); offset < state.log.size(); ++offset) {
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
    // This project's configuration is always three members (majority = two).
    std::array<LogIndex, 3> matched{status().last_log_index, peers_[0].match_index, peers_[1].match_index};
    std::sort(matched.begin(), matched.end());
    const auto candidate = matched[1];
    const auto& log = persistent_state().log;
    // Counting replicas alone cannot commit an entry from an older term.
    if (candidate > commit_index_ && log[candidate - 1].term == persistent_state().current_term) {
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
        const auto& log = persistent_state().log;
        const bool prefix_matches = request.prev_log_index == 0 ||
            (request.prev_log_index <= log.size() && log[request.prev_log_index - 1].term == request.prev_log_term);
        if (prefix_matches) {
            std::size_t different = 0;
            while (different < request.entries.size()) {
                const auto& incoming = request.entries[different];
                if (incoming.index > log.size() || log[incoming.index - 1].term != incoming.term) { break; }
                if (!detail::SameEntry(log[incoming.index - 1], incoming)) {
                    throw std::runtime_error("Raft index and term identify different commands");
                }
                ++different;
            }
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
    AdvanceCommit();
    for (auto& target : peers_) {
        if (target.next_index <= status().last_log_index || target.acknowledged_commit < commit_index_) {
            SendAppend(target);
        }
    }
}

}  // namespace kvstore::raft
