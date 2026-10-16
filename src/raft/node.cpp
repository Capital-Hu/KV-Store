#include "kvstore/raft/node.h"
#include "kvstore/raft/storage.h"
#include "storage_codec.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace kvstore::raft {

Node::Node(NodeConfig config) : Node(std::move(config), {}) {}

Node::Node(NodeConfig config, std::unique_ptr<Storage> storage)
    : config_(std::move(config)), storage_(std::move(storage)) {
    detail::ValidateConfig(config_);
    if (storage_) {
        auto peers = config_.peers, stored_peers = storage_->config().peers;
        std::sort(peers.begin(), peers.end()); std::sort(stored_peers.begin(), stored_peers.end());
        if (storage_->config().id != config_.id || peers != stored_peers) {
            throw std::invalid_argument("Raft node and storage identities differ");
        }
        storage_->state();
    }
}

Node::~Node() = default;

const PersistentState& Node::persistent_state() const {
    return storage_ ? storage_->state() : initial_;
}

NodeStatus Node::status() const {
    const auto& state = persistent_state();
    return {config_.id, role_, state.current_term, leader_id_,
            state.log.empty() ? 0 : state.log.back().index,
            state.log.empty() ? 0 : state.log.back().term,
            commit_index_, last_applied_, logical_time_};
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
}

}  // namespace kvstore::raft
