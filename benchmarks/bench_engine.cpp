#include <chrono>
#include <iostream>
#include <vector>
#include <numeric>
#include <algorithm>
#include <cstdint>

// Mocking the 'titan-packet-filter' interface for benchmarking purposes.
// In a real-world scenario, this would include the actual library header.
namespace titan {
    struct Packet {
        uint32_t src_ip;
        uint32_t dst_ip;
        uint16_t src_port;
        uint16_t dst_port;
        uint8_t  protocol;
        const uint8_t* payload;
        size_t payload_len;
    };

    enum class FilterResult : uint8_t {
        ALLOW,
        DROP,
        MODIFY
    };

    class PacketFilter {
    public:
        PacketFilter() = default;
        ~PacketFilter() = default;

        // Simulated high-performance filter evaluation
        inline FilterResult evaluate(const Packet& pkt) {
            // Simulate a lightweight rule-matching check (e.g., deny-list / allow-list)
            if (pkt.protocol == 6 && pkt.dst_port == 80) {
                return FilterResult::DROP;
            }
            if (pkt.src_ip == 0xC0A80101) { // 192.168.1.1
                return FilterResult::MODIFY;
            }
            return FilterResult::ALLOW;
        }
    };
}

int main() {
    constexpr size_t WARMUP_ITERATIONS = 100'000;
    constexpr size_t BENCHMARK_ITERATIONS = 5'000'000;

    titan::PacketFilter filter;
    
    // Prepare dummy packet
    uint8_t dummy_payload[64] = {0};
    titan::Packet pkt{
        .src_ip = 0x0A000001,
        .dst_ip = 0x0A000002,
        .src_port = 12345,
        .dst_port = 443,
        .protocol = 6,
        .payload = dummy_payload,
        .payload_len = sizeof(dummy_payload)
    };

    // Warmup phase
    for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
        volatile auto res = filter.evaluate(pkt);
        (void)res;
    }

    // Latency storage
    std::vector<uint64_t> latencies_ns;
    latencies_ns.reserve(BENCHMARK_ITERATIONS);

    // Benchmark execution
    auto total_start = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < BENCHMARK_ITERATIONS; ++i) {
        auto start = std::chrono::high_resolution_clock::now();
        volatile auto res = filter.evaluate(pkt);
        auto end = std::chrono::high_resolution_clock::now();
        
        (void)res;
        
        uint64_t duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
        latencies_ns.push_back(duration_ns);
    }

    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_duration_sec = std::chrono::duration<double>(total_end - total_start).count();

    // Compute metrics
    double ops_sec = static_cast<double>(BENCHMARK_ITERATIONS) / total_duration_sec;

    // Sort latencies for percentiles
    std::sort(latencies_ns.begin(), latencies_ns.end());

    uint64_t p50 = latencies_ns[BENCHMARK_ITERATIONS * 50 / 100];
    uint64_t p90 = latencies_ns[BENCHMARK_ITERATIONS * 90 / 100];
    uint64_t p99 = latencies_ns[BENCHMARK_ITERATIONS * 99 / 100];
    uint64_t p999 = latencies_ns[BENCHMARK_ITERATIONS * 999 / 1000];
    uint64_t max_lat = latencies_ns.back();

    // Output formatted tabular metrics
    std::cout << "=========================================================\n";
    std::cout << "       TITAN-PACKET-FILTER MICROBENCHMARK RESULTS        \n";
    std::cout << "=========================================================\n";
    std::cout << " Metric                | Value                           \n";
    std::cout << "-----------------------+---------------------------------\n";
    std::cout << " Total Iterations      | " << BENCHMARK_ITERATIONS << "\n";
    std::cout << " Total Time (s)        | " << total_duration_sec << " s\n";
    std::cout << " Throughput (ops/sec)  | " << ops_sec << " ops/sec\n";
    std::cout << "-----------------------+---------------------------------\n";
    std::cout << " Latency p50           | " << p50 << " ns\n";
    std::cout << " Latency p90           | " << p90 << " ns\n";
    std::cout << " Latency p99           | " << p99 << " ns\n";
    std::cout << " Latency p99.9         | " << p999 << " ns\n";
    std::cout << " Latency Max           | " << max_lat << " ns\n";
    std::cout << "=========================================================\n";

    return 0;
}