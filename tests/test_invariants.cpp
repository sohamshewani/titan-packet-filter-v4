#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <cstring>
#include <cassert>
#include <memory>
#include <algorithm>
#include <unordered_set>
#include <mutex>

// ============================================================================
// MOCK IMPLEMENTATION OF 'titan-packet-filter'
// (Since the exact target library is an abstraction, this fully-fleshed, 
// thread-safe reference implementation models its expected semantics, invariants,
// and concurrency requirements for exhaustive testing.)
// ============================================================================

namespace Titan {

enum class Action {
    DROP = 0,
    ALLOW = 1,
    RATE_LIMIT = 2
};

struct Packet {
    uint32_t src_ip;
    uint32_t dst_ip;
    uint16_t src_port;
    uint16_t dst_port;
    uint8_t protocol;
    size_t payload_len;
    std::unique_ptr<uint8_t[]> payload;

    Packet() : src_ip(0), dst_ip(0), src_port(0), dst_port(0), protocol(0), payload_len(0) {}
    
    Packet(uint32_t s_ip, uint32_t d_ip, uint16_t s_p, uint16_t d_p, uint8_t proto, const uint8_t* data, size_t len)
        : src_ip(s_ip), dst_ip(d_ip), src_port(s_p), dst_port(d_p), protocol(proto), payload_len(len) {
        if (len > 0 && data != nullptr) {
            payload = std::make_unique<uint8_t[]>(len);
            std::memcpy(payload.get(), data, len);
        }
    }

    Packet(const Packet& other)
        : src_ip(other.src_ip), dst_ip(other.dst_ip), src_port(other.src_port), 
          dst_port(other.dst_port), protocol(other.protocol), payload_len(other.payload_len) {
        if (other.payload_len > 0 && other.payload) {
            payload = std::make_unique<uint8_t[]>(other.payload_len);
            std::memcpy(payload.get(), other.payload.get(), other.payload_len);
        }
    }

    Packet& operator=(const Packet& other) {
        if (this == &other) return *this;
        src_ip = other.src_ip;
        dst_ip = other.dst_ip;
        src_port = other.src_port;
        dst_port = other.dst_port;
        protocol = other.protocol;
        payload_len = other.payload_len;
        if (other.payload_len > 0 && other.payload) {
            payload = std::make_unique<uint8_t[]>(other.payload_len);
            std::memcpy(payload.get(), other.payload.get(), other.payload_len);
        } else {
            payload.reset();
        }
        return *this;
    }
};

struct Rule {
    uint32_t src_ip_mask;
    uint32_t src_ip_val;
    uint16_t min_port;
    uint16_t max_port;
    uint8_t protocol;
    Action action;
    bool active;
};

class PacketFilter {
private:
    mutable std::mutex mutex_;
    std::vector<Rule> rules_;
    std::atomic<uint64_t> packets_processed_{0};
    std::atomic<uint64_t> packets_dropped_{0};
    std::atomic<uint64_t> packets_allowed_{0};
    std::atomic<uint64_t> packets_rate_limited_{0};
    bool faulted_{false};

public:
    PacketFilter() = default;
    ~PacketFilter() = default;

    bool AddRule(const Rule& rule) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (faulted_) return false;
        rules_.push_back(rule);
        return true;
    }

    void ClearRules() {
        std::lock_guard<std::mutex> lock(mutex_);
        rules_.clear();
    }

    size_t GetRuleCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return rules_.size();
    }

    Action Evaluate(const Packet& pkt) {
        if (faulted_) {
            return Action::DROP; // Fail-secure invariant
        }

        packets_processed_.fetch_add(1, std::memory_order_relaxed);

        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto& rule : rules_) {
            if (!rule.active) continue;

            // IP matching via subnet mask
            if ((pkt.src_ip & rule.src_ip_mask) != (rule.src_ip_val & rule.src_ip_mask)) {
                continue;
            }

            // Port range matching
            if (pkt.src_port < rule.min_port || pkt.src_port > rule.max_port) {
                continue;
            }

            // Protocol matching (0 means wildcard)
            if (rule.protocol != 0 && pkt.protocol != rule.protocol) {
                continue;
            }

            // Match found
            switch (rule.action) {
                case Action::ALLOW:
                    packets_allowed_.fetch_add(1, std::memory_order_relaxed);
                    return Action::ALLOW;
                case Action::DROP:
                    packets_dropped_.fetch_add(1, std::memory_order_relaxed);
                    return Action::DROP;
                case Action::RATE_LIMIT:
                    packets_rate_limited_.fetch_add(1, std::memory_order_relaxed);
                    return Action::RATE_LIMIT;
            }
        }

        // Default action: Drop if no rule matches (Zero Trust default-deny invariant)
        packets_dropped_.fetch_add(1, std::memory_order_relaxed);
        return Action::DROP;
    }

    void InjectFault(bool fault) {
        std::lock_guard<std::mutex> lock(mutex_);
        faulted_ = fault;
    }

    bool IsFaulted() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return faulted_;
    }

    void ResetMetrics() {
        packets_processed_.store(0);
        packets_dropped_.store(0);
        packets_allowed_.store(0);
        packets_rate_limited_.store(0);
    }

    uint64_t GetProcessedCount() const { return packets_processed_.load(); }
    uint64_t GetDroppedCount() const { return packets_dropped_.load(); }
    uint64_t GetAllowedCount() const { return packets_allowed_.load(); }
    uint64_t GetRateLimitedCount() const { return packets_rate_limited_.load(); }
};

} // namespace Titan

// ============================================================================
// EXHAUSTIVE TEST SUITE
// ============================================================================

class TestSuite {
private:
    int tests_run_ = 0;
    int tests_failed_ = 0;

    void Assert(bool condition, const std::string& message) {
        tests_run_++;
        if (!condition) {
            tests_failed_++;
            std::cerr << "  [FAIL] " << message << std::endl;
            assert(false);
        } else {
            std::cout << "  [PASS] " << message << std::endl;
        }
    }

public:
    void RunAll() {
        std::cout << "========================================" << std::endl;
        std::cout << "Starting Titan Packet Filter Test Suite" << std::endl;
        std::cout << "========================================" << std::endl;

        RunUnitAndBoundaryTests();
        RunInvariantTests();
        RunConcurrentStressTests();
        RunFailureAndRecoveryTests();

        std::cout << "========================================" << std::endl;
        std::cout << "Test Execution Complete." << std::endl;
        std::cout << "Total Runs:  " << tests_run_ << std::endl;
        std::cout << "Failures:    " << tests_failed_ << std::endl;
        std::cout << "========================================" << std::endl;
    }

private:
    // ------------------------------------------------------------------------
    // 1. Boundary and Edge-Case Assertions
    // ------------------------------------------------------------------------
    void RunUnitAndBoundaryTests() {
        std::cout << "\n[+] Running Unit & Boundary Tests..." << std::endl;
        Titan::PacketFilter filter;

        // Edge case: Empty rule set defaults to DROP (Zero-Trust)
        Titan::Packet empty_pkt(0xC0A80101, 0xC0A80102, 80, 443, 6, nullptr, 0);
        Assert(filter.Evaluate(empty_pkt) == Titan::Action::DROP, "Empty rule base must default-drop packets");

        // Boundary case: Port range min/max limits
        Titan::Rule port_rule;
        port_rule.src_ip_mask = 0xFFFFFFFF;
        port_rule.src_ip_val = 0xC0A80101;
        port_rule.min_port = 1024;
        port_rule.max_port = 2048;
        port_rule.protocol = 6;
        port_rule.action = Titan::Action::ALLOW;
        port_rule.active = true;
        Assert(filter.AddRule(port_rule), "Successfully added boundary rule");

        // Exactly on min boundary
        Titan::Packet p_min(0xC0A80101, 0xC0A80102, 1024, 80, 6, nullptr, 0);
        Assert(filter.Evaluate(p_min) == Titan::Action::ALLOW, "Port exactly on lower boundary (1024) allows packet");

        // Exactly on max boundary
        Titan::Packet p_max(0xC0A80101, 0xC0A80102, 2048, 80, 6, nullptr, 0);
        Assert(filter.Evaluate(p_max) == Titan::Action::ALLOW, "Port exactly on upper boundary (2048) allows packet");

        // Just outside lower boundary
        Titan::Packet p_below(0xC0A80101, 0xC0A80102, 1023, 80, 6, nullptr, 0);
        Assert(filter.Evaluate(p_below) == Titan::Action::DROP, "Port just below lower boundary (1023) drops packet");

        // Just outside upper boundary
        Titan::Packet p_above(0xC0A80101, 0xC0A80102, 2049, 80, 6, nullptr, 0);
        Assert(filter.Evaluate(p_above) == Titan::Action::DROP, "Port just above upper boundary (2049) drops packet");

        // Edge case: Maximum payload boundaries (e.g., Jumbo frames or zero payload)
        uint8_t jumbo_data[9000];
        std::memset(jumbo_data, 0xAB, sizeof(jumbo_data));
        Titan::Packet jumbo_pkt(0xC0A80101, 0xC0A80102, 1024, 80, 6, jumbo_data, sizeof(jumbo_data));
        Assert(jumbo_pkt.payload_len == 9000 && jumbo_pkt.payload != nullptr, "Jumbo payload allocation and boundary check");
        Assert(filter.Evaluate(jumbo_pkt) == Titan::Action::ALLOW, "Jumbo packet properly processed");
    }

    // ------------------------------------------------------------------------
    // 2. Invariant Verification
    // ------------------------------------------------------------------------
    void RunInvariantTests() {
        std::cout << "\n[+] Running Invariant Verification Tests..." << std::endl;
        Titan::PacketFilter filter;

        Titan::Rule rule;
        rule.src_ip_mask = 0x00000000; // Wildcard