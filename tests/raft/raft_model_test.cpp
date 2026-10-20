#include "kvstore/raft/node.h"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using namespace std::chrono_literals;
namespace raft = kvstore::raft;

void Check(bool condition, const std::string& message) {
    if (!condition) { throw std::runtime_error(message); }
}

template <typename Error, typename Action>
void CheckThrows(Action action, const std::string& message) {
    try { action(); }
    catch (const Error&) { return; }
    throw std::runtime_error(message);
}

void CheckInitialProtocolState(const raft::Node& node) {
    const auto status = node.status();
    Check(status.role == raft::Role::Follower && status.term == 0, "Fresh nodes start as term-zero followers");
    Check(!status.leader_id && !node.persistent_state().voted_for, "No known leader or vote at bootstrap");
    Check(node.persistent_state().log.empty(), "Bootstrap does not invent a log entry");
    Check(status.last_log_index == 0 && status.last_log_term == 0, "Empty log has an index-zero prefix");
    Check(status.commit_index == 0 && status.last_applied == 0, "Bootstrap commits or applies nothing");
}

void TestFreshThreeNodeState() {
    const std::vector<raft::NodeConfig> configs{{1, {2, 3}}, {2, {1, 3}}, {3, {1, 2}}};
    for (const auto& config : configs) {
        const raft::Node node(config);
        Check(node.status().id == config.id, "Each node retains its own identity");
        Check(node.config().peers == config.peers, "Peers exclude this node");
        Check(node.majority_size() == 2, "A three-node majority includes two members");
        Check(node.status().logical_time == 0ms, "Each logical clock starts at zero");
        CheckInitialProtocolState(node);
    }
}

void TestInvalidMembership() {
    const std::vector<raft::NodeConfig> invalid{
        {0, {1, 2}}, {1, {2}}, {1, {2, 3, 4}},
        {1, {0, 2}}, {1, {2, 0}}, {1, {1, 2}}, {1, {2, 1}}, {1, {2, 2}}};
    for (const auto& config : invalid) {
        CheckThrows<std::invalid_argument>([&] { raft::Node node(config); }, "Reject invalid cluster membership");
    }
}

void TestManualTimeIsLocalAndDeterministic() {
    raft::Node first({1, {2, 3}}), second({2, {1, 3}}), third({3, {1, 2}});
    first.AdvanceTime(100ms);
    Check(first.status().logical_time == 100ms && second.status().logical_time == 0ms,
          "Advancing one node does not advance another");
    first.AdvanceTime(150ms);
    second.AdvanceTime(250ms);
    Check(first.status().logical_time == second.status().logical_time, "Clock increments compose deterministically");
    first.AdvanceTime(0ms);
    Check(first.status().logical_time == 250ms && third.status().logical_time == 0ms,
          "Zero increments and untouched clocks preserve time");
    third.AdvanceTime(1h);
    CheckInitialProtocolState(first);
    CheckInitialProtocolState(second);
    CheckInitialProtocolState(third);  // This step has no election timeout handler yet.
}

void TestInvalidTimeAdvancePreservesState() {
    raft::Node node({1, {2, 3}});
    node.AdvanceTime(7ms);
    CheckThrows<std::invalid_argument>([&] { node.AdvanceTime(-1ms); }, "Reject negative elapsed time");
    Check(node.status().logical_time == 7ms, "Negative elapsed time leaves state unchanged");
    CheckThrows<std::overflow_error>([&] { node.AdvanceTime(std::chrono::milliseconds::max()); },
                                     "Reject time overflow before adding");
    Check(node.status().logical_time == 7ms, "Overflow leaves state unchanged");
    node.AdvanceTime(std::chrono::milliseconds::max() - 7ms);
    node.AdvanceTime(0ms);
    Check(node.status().logical_time == std::chrono::milliseconds::max(), "Largest representable time is valid");
    CheckThrows<std::overflow_error>([&] { node.AdvanceTime(1ms); }, "Reject time beyond the limit");
    Check(node.status().logical_time == std::chrono::milliseconds::max(), "Overflow never wraps logical time");
    CheckInitialProtocolState(node);
}
}  // namespace

int main() {
    try {
        TestFreshThreeNodeState();
        TestInvalidMembership();
        TestManualTimeIsLocalAndDeterministic();
        TestInvalidTimeAdvancePreservesState();
        std::cout << "All 4 Raft model tests passed.\n";
    } catch (const std::exception& error) {
        std::cerr << "Raft model test failed: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
