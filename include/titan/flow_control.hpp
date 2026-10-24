#ifndef TITAN_FLOW_CONTROL_HPP
#ifndef TITAN_FLOW_CONTROL_HPP
#define TITAN_FLOW_CONTROL_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <array>
#include <chrono>
#include <new>
#include <system_error>

namespace titan {

enum class FlowControlError : uint8_t {
    Success = 0,
    RingBufferFull,
    RingBufferEmpty,
    ArenaExhausted,
    InvalidSequence,
    BucketExpired
};

class FlowControlCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan_flow_control";
    }

    std::string message(int ev) const override {
        switch (static_cast<FlowControlError>(ev)) {
            case FlowControlError::Success:
                return "Success";
            case FlowControlError::RingBufferFull:
                return "Ring buffer capacity reached (Backpressure engaged)";
            case FlowControlError::RingBufferEmpty:
                return "Ring buffer empty";
            case FlowControlError::ArenaExhausted:
                return "Memory arena exhausted (Epoch reclamation triggered)";
            case FlowControlError::InvalidSequence:
                return "Invalid packet sequence index";
            case FlowControlError::BucketExpired:
                return "Sliding-window time bucket expired";
            default:
                return "Unknown flow control error";
        }
    }
};

inline const std::error_category& get_flow_control_category() noexcept {
    static FlowControlCategory instance;
    return instance;
}

inline std::error_code make_error_code(FlowControlError e) noexcept {
    return {static_cast<int>(e), get_flow_control_category()};
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::FlowControlError> : true_type {};
} // namespace std

namespace titan {

class alignas(64) ArenaAllocator {
public:
    static constexpr size_t kBlockSize = 64;
    
    explicit ArenaAllocator(size_t total_blocks)
        : total_blocks_(total_blocks),
          current_offset_(0),
          arena_memory_(nullptr) {
        size_t total_bytes = total_blocks_ * kBlockSize;
        int alloc_result = posix_memalign(reinterpret_cast<void**>(&arena_memory_), kBlockSize, total_bytes);
        if (alloc_result != 0 || arena_memory_ == nullptr) {
            throw std::system_error(alloc_result, std::system_category(), "Failed to allocate aligned arena memory");
        }
    }

    ~ArenaAllocator() noexcept {
        if (arena_memory_ != nullptr) {
            free(arena_memory_);
            arena_memory_ = nullptr;
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&& other) noexcept
        : total_blocks_(other.total_blocks_),
          current_offset_(other.current_offset_.load(std::memory_order_relaxed)),
          arena_memory_(other.arena_memory_) {
        other.arena_memory_ = nullptr;
        other.total_blocks_ = 0;
        other.current_offset_.store(0, std::memory_order_relaxed);
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (arena_memory_ != nullptr) {
                free(arena_memory_);
            }
            total_blocks_ = other.total_blocks_;
            current_offset_.store(other.current_offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            arena_memory_ = other.arena_memory_;
            other.arena_memory_ = nullptr;
            other.total_blocks_ = 0;
            other.current_offset_.store(0, std::memory_order_relaxed);
        }
        return *this;
    }

    void* allocate_block() noexcept {
        size_t current = current_offset_.load(std::memory_order_relaxed);
        while (true) {
            if (current >= total_blocks_) {
                return nullptr;
            }
            if (current_offset_.compare_exchange_weak(current, current + 1,
                                                      std::memory_order_acquire,
                                                      std::memory_order_relaxed)) {
                return static_cast<void*>(arena_memory_ + (current * kBlockSize));
            }
        }
    }

    void reset() noexcept {
        current_offset_.store(0, std::memory_order_release);
    }

    void force_reclaim_epoch(size_t target_blocks) noexcept {
        size_t current = current_offset_.load(std::memory_order_relaxed);
        while (true) {
            size_t new_val = (current >= target_blocks) ? (current - target_blocks) : 0;
            if (current_offset_.compare_exchange_weak(current, new_val,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_relaxed)) {
                break;
            }
        }
    }

    size_t available_blocks() const noexcept {
        size_t current = current_offset_.load(std::memory_order_relaxed);
        return (current < total_blocks_) ? (total_blocks_ - current) : 0;
    }

private:
    size_t total_blocks_;
    std::atomic<size_t> current_offset_;
    char* arena_memory_;
};

template <typename T, size_t Capacity>
class alignas(64) SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

public:
    SPSCRingBuffer() : head_(0), tail_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i] = nullptr;
        }
    }

    ~SPSCRingBuffer() = default;

    SPSCRingBuffer(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;

    bool push(const T* item) noexcept {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false;
        }

        buffer_[current_tail & kMask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T*& item) noexcept {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        const size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false;
        }

        item = const_cast<T*>(buffer_[current_head & kMask]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    size_t size() const noexcept {
        size_t head_val = head_.load(std::memory_order_acquire);
        size_t tail_val = tail_.load(std::memory_order_acquire);
        if (tail_val >= head_val) {
            return tail_val - head_val;
        }
        return 0;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

private:
    static constexpr size_t kMask = Capacity - 1;

    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    alignas(64) std::array<const T*, Capacity> buffer_;
};

template <size_t NumBuckets = 64>
class alignas(64) SlidingWindowFlowControl {
public:
    struct alignas(64) TimeBucket {
        std::atomic<uint64_t> accept_count{0};
        std::atomic<uint64_t> drop_count{0};
        std::atomic<uint64_t> epoch_timestamp{0};
    };

    SlidingWindowFlowControl() : current_bucket_index_(0) {
        uint64_t initial_time = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now().time_since_epoch()
            ).count()
        );
        for (size_t i = 0; i < NumBuckets; ++i) {
            buckets_[i].epoch_timestamp.store(initial_time, std::memory_order_relaxed);
            buckets_[i].accept_count.store(0, std::memory_order_relaxed);
            buckets_[i].drop_count.store(0, std::memory_order_relaxed);
        }
    }

    ~SlidingWindowFlowControl() = default;

    SlidingWindowFlowControl(const SlidingWindowFlowControl&) = delete;
    SlidingWindowFlowControl& operator=(const SlidingWindowFlowControl&) = delete;

    void record_accept(uint64_t timestamp_ms) noexcept {
        size_t idx = get_bucket_index(timestamp_ms);
        buckets_[idx].accept_count.fetch_add(1, std::memory_order_relaxed);
    }

    void record_drop(uint64_t timestamp_ms) noexcept {
        size_t idx = get_bucket_index(timestamp_ms);
        buckets_[idx].drop_count.fetch_add(1, std::memory_order_relaxed);
    }

    bool evaluate_red(uint64_t timestamp_ms, double drop_probability_threshold) noexcept {
        size_t idx = get_bucket_index(timestamp_ms);
        TimeBucket& bucket = buckets_[idx];

        uint64_t accepts = bucket.accept_count.load(std::memory_order_relaxed);
        uint64_t drops = bucket.drop_count.load(std::memory_order_relaxed);
        uint64_t total = accepts + drops;

        if (total < 100) {
            return false;
        }

        double current_drop_rate = static_cast<double>(drops) / static_cast<double>(total);
        return current_drop_rate >= drop_probability_threshold;
    }

    void aggregate_metrics(uint64_t& total_accepts, uint64_t& total_drops) const noexcept {
        total_accepts = 0;
        total_drops = 0;
        for (size_t i = 0; i < NumBuckets; ++i) {
            total_accepts += buckets_[i].accept_count.load(std::memory_order_relaxed);
            total_drops += buckets_[i].drop_count.load(std::memory_order_relaxed);
        }
    }

private:
    size_t get_bucket_index(uint64_t timestamp_ms) noexcept {
        constexpr uint64_t kBucketIntervalMs = 1000;
        size_t idx = (timestamp_ms / kBucketIntervalMs) % NumBuckets;
        
        uint64_t expected_epoch = buckets_[idx].epoch_timestamp.load(std::memory_order_relaxed);
        uint64_t current_epoch_bucket = timestamp_ms / kBucketIntervalMs;

        if (expected_epoch != current_epoch_bucket) {
            if (buckets_[idx].epoch_timestamp.compare_exchange_strong(
                    expected_epoch, current_epoch_bucket,
                    std::memory_order_acq_rel, std::memory_order_relaxed)) {
                buckets_[idx].accept_count.store(0, std::memory_order_relaxed);
                buckets_[idx].drop_count.store(0, std::memory_order_relaxed);
            }
        }

        return idx;
    }

    alignas(64) std::array<TimeBucket, NumBuckets> buckets_;
    std::atomic<size_t> current_bucket_index_;
};

} // namespace titan

#endif // TITAN_FLOW_CONTROL_HPP
#endif // TITAN_FLOW_CONTROL_HPP