#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"
#include "storage_codec.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace kvstore::raft {

Node::Node(NodeConfig config) : Node(std::move(config), {}) {}

Node::Node(NodeConfig config, std::unique_ptr<Storage> storage, ElectionOptions options)
    : config_(std::move(config)), storage_(std::move(storage)), options_(options) {
    detail::ValidateConfig(config_);
    if (options_.heartbeat_interval.count() <= 0 || options_.election_timeout_min <= options_.heartbeat_interval ||
        options_.election_timeout_max < options_.election_timeout_min) {
        throw std::invalid_argument("Election timeout must exceed a positive heartbeat interval");
    }
    if (storage_) {
        auto peers = config_.peers, stored_peers = storage_->config().peers;
        std::sort(peers.begin(), peers.end()); std::sort(stored_peers.begin(), stored_peers.end());
        if (storage_->config().id != config_.id || peers != stored_peers) {
            throw std::invalid_argument("Raft node and storage identities differ");
        }
        storage_->state();
        if (options_.random_seed) { random_.seed(*options_.random_seed); }
        else {
            std::random_device entropy;
            random_.seed((static_cast<std::uint64_t>(entropy()) << 32) ^ entropy() ^ config_.id);
        }
        ResetElectionTimer();
    }
    for (const auto id : config_.peers) { peers_.push_back({id}); }
}

Node::~Node() = default;

const PersistentState& Node::persistent_state() const {
    EnsureUsable();
    return storage_ ? storage_->state() : initial_;
}

bool Node::usable() const { return usable_ && (!storage_ || storage_->usable()); }

void Node::EnsureUsable() const {
    if (!usable()) { throw std::runtime_error("Raft node failed; close and reopen before reuse"); }
}

void Node::Fail() { usable_ = false; outbox_.clear(); }

NodeStatus Node::status() const {
    const auto& state = persistent_state();
    return {config_.id, role_, state.current_term, leader_id_,
            state.log.empty() ? 0 : state.log.back().index,
            state.log.empty() ? 0 : state.log.back().term,
            commit_index_, last_applied_, logical_time_, election_elapsed_, election_timeout_, heartbeat_elapsed_};
}

void Node::AdvanceTime(std::chrono::milliseconds elapsed) {
    persistent_state();
    if (elapsed.count() < 0) {
        throw std::invalid_argument("Raft logical time cannot move backwards");
    }
    if (elapsed > std::chrono::milliseconds::max() - logical_time_) {
        throw std::overflow_error("Raft logical time overflow");
    }
    logical_time_ += elapsed;
    if (!storage_) { return; }
    try {
        if (role_ == Role::Leader) {
            if (elapsed >= options_.heartbeat_interval - heartbeat_elapsed_) {
                heartbeat_elapsed_ = std::chrono::milliseconds{0};
                SendHeartbeats();
            } else { heartbeat_elapsed_ += elapsed; }
        } else {
            if (elapsed >= election_timeout_ - election_elapsed_) { StartElection(); }
            else { election_elapsed_ += elapsed; }
        }
    } catch (...) { Fail(); throw; }
}

std::vector<Message> Node::DrainMessages() {
    EnsureUsable();
    std::vector<Message> result;
    result.swap(outbox_);
    return result;
}

void Node::Step(const Message& message) {
    EnsureUsable();
    if (!storage_) { throw std::logic_error("Protocol messages require durable Raft storage"); }
    if (message.to != config_.id || message.rpc_id == 0 ||
        std::find(config_.peers.begin(), config_.peers.end(), message.from) == config_.peers.end()) { return; }
    try {
        std::visit([&](const auto& body) { Handle(message, body); }, message.body);
    } catch (...) { Fail(); throw; }
}

void Node::ResetElectionTimer() {
    using Milliseconds = std::chrono::milliseconds;
    std::uniform_int_distribution<Milliseconds::rep> distribution(
        options_.election_timeout_min.count(), options_.election_timeout_max.count());
    election_timeout_ = Milliseconds{distribution(random_)};
    election_elapsed_ = Milliseconds{0};
}

void Node::ClearElection() {
    outbox_.clear();
    for (auto& peer : peers_) {
        peer.vote_rpc_id = 0; peer.vote_granted = false;
        peer.pending.reset(); peer.next_index = 1; peer.match_index = 0; peer.acknowledged_commit = 0;
    }
}

void Node::ObserveHigherTerm(Term term) {
    if (term <= persistent_state().current_term) { return; }
    storage_->SetTermAndVote(term, std::nullopt);
    role_ = Role::Follower;
    leader_id_.reset();
    heartbeat_elapsed_ = std::chrono::milliseconds{0};
    ClearElection();
    // A denied vote or stale response must not postpone this node's next election.
}

std::uint64_t Node::NextRpcId() {
    if (last_rpc_id_ == std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error("Raft RPC identifiers exhausted");
    }
    return ++last_rpc_id_;
}

void Node::StartElection() {
    const auto term = persistent_state().current_term;
    if (term == std::numeric_limits<Term>::max()) { throw std::overflow_error("Raft term exhausted"); }
    // The self-vote and new term must be durable before any request becomes sendable.
    storage_->SetTermAndVote(term + 1, config_.id);
    role_ = Role::Candidate;
    leader_id_.reset();
    ClearElection();
    ResetElectionTimer();
    const auto current = status();
    for (auto& peer : peers_) {
        peer.vote_rpc_id = NextRpcId();
        outbox_.push_back({config_.id, peer.id, peer.vote_rpc_id,
                          RequestVoteRequest{current.term, config_.id, current.last_log_index, current.last_log_term}});
    }
}

void Node::BecomeLeader() {
    role_ = Role::Leader;
    leader_id_ = config_.id;
    ClearElection();
    election_elapsed_ = heartbeat_elapsed_ = std::chrono::milliseconds{0};
    const auto next = status().last_log_index + 1;
    for (auto& peer : peers_) { peer.next_index = next; }
    // A current-term entry lets the new leader establish the committed prefix.
    storage_->Append({{next, status().term, {CommandType::NoOp, {}, {}}}});
    SendHeartbeats();
}

void Node::Handle(const Message& message, const RequestVoteRequest& request) {
    if (request.candidate_id != message.from || request.term == 0 || request.last_log_term > request.term ||
        ((request.last_log_index == 0) != (request.last_log_term == 0))) { return; }
    ObserveHigherTerm(request.term);
    const auto current = status();
    const auto vote = persistent_state().voted_for;
    const bool fresh = request.last_log_term > current.last_log_term ||
        (request.last_log_term == current.last_log_term && request.last_log_index >= current.last_log_index);
    const bool grant = request.term == current.term && fresh && (!vote || *vote == request.candidate_id);
    if (grant) {
        storage_->SetTermAndVote(current.term, request.candidate_id);
        ResetElectionTimer();
    }
    outbox_.push_back({config_.id, message.from, message.rpc_id, RequestVoteResponse{current.term, grant}});
}

void Node::Handle(const Message& message, const RequestVoteResponse& response) {
    ObserveHigherTerm(response.term);
    if (role_ != Role::Candidate || response.term != persistent_state().current_term || !response.vote_granted) { return; }
    const auto peer = std::find_if(peers_.begin(), peers_.end(), [&](const Peer& value) { return value.id == message.from; });
    if (peer->vote_rpc_id != message.rpc_id) { return; }
    peer->vote_granted = true;
    const auto votes = 1 + static_cast<std::size_t>(std::count_if(peers_.begin(), peers_.end(),
        [](const Peer& value) { return value.vote_granted; }));
    if (votes >= majority_size()) { BecomeLeader(); }
}

}  // namespace kvstore::raft
