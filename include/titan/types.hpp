#ifndef TITAN_TYPES_HPP
#define TITAN_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <new>
#include <array>
#include <system_error>
#include <chrono>

namespace titan {

// Hardware cache line size for modern x86_64 and ARM architectures
constexpr std::size_t kCacheLineSize = 64;

// System error codes for Titan Packet Filter
enum class ErrorCode : uint32_t {
    Success = 0,
    ArenaExhausted,
    RingBufferFull,
    RingBufferEmpty,
    InvalidParameter,
    StateCorruption
};

class TitanErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan";
    }

    std::string message(int ev) const override {
        switch (static_cast<ErrorCode>(ev)) {
            case ErrorCode::Success: return "Success";
            case ErrorCode::ArenaExhausted: return "Packet buffer arena exhausted";
            case ErrorCode::RingBufferFull: return "SPSC ring buffer is full";
            case ErrorCode::RingBufferEmpty: return "SPSC ring buffer is empty";
            case ErrorCode::InvalidParameter: return "Invalid parameter supplied";
            case ErrorCode::StateCorruption: return "Critical memory or state corruption detected";
            default: return "Unknown titan error";
        }
    }
};

inline const std::error_category& get_titan_category() noexcept {
    static TitanErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ErrorCode e) noexcept {
    return {static_cast<int>(e), get_titan_category()};
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::ErrorCode> : true_type {};
} // namespace std

namespace titan {

// 1. PACKET BUFFER ARENA: Fixed-size contiguous memory arena utilizing cache-aligned 64-byte blocks
template <std::size_t BlockSize = 2048, std::size_t BlockCount = 4096>
class ArenaAllocator {
public:
    static_assert(BlockSize >= kCacheLineSize, "BlockSize must be at least cache line size");
    static_assert((BlockSize % kCacheLineSize) == 0, "BlockSize must be a multiple of cache line size");

    struct alignas(kCacheLineSize) Block {
        alignas(kCacheLineSize) std::array<uint8_t, BlockSize> data;
    };

    ArenaAllocator() noexcept 
        : next_free_index_(0) {
        for (std::size_t i = 0; i < BlockCount - 1; ++i) {
            *reinterpret_cast<std::size_t*>(&free_list_storage_[i * kCacheLineSize]) = i + 1;
        }
        if (BlockCount > 0) {
            *reinterpret_cast<std::size_t*>(&free_list_storage_[(BlockCount - 1) * kCacheLineSize]) = kInvalidIndex;
        }
    }

    ~ArenaAllocator() = default;

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = delete;

    [[nodiscard]] uint8_t* allocate() noexcept {
        std::size_t current_head = next_free_index_.load(std::memory_order_relaxed);
        while (current_head != kInvalidIndex) {
            std::size_t next_head = *reinterpret_cast<const std::size_t*>(&free_list_storage_[current_head * kCacheLineSize]);
            if (next_free_index_.compare_exchange_weak(current_head, next_head, 
                                                       std::memory_order_acquire, 
                                                       std::memory_order_relaxed)) {
                return blocks_[current_head].data.data();
            }
        }
        return nullptr; // Arena exhausted; triggers graceful degradation / epoch reclamation
    }

    void deallocate(uint8_t* ptr) noexcept {
        if (ptr == nullptr) {
            return;
        }
        
        uintptr_t ptr_val = reinterpret_cast<uintptr_t>(ptr);
        uintptr_t base_val = reinterpret_cast<uintptr_t>(blocks_.data());
        
        if (ptr_val < base_val) {
            return;
        }

        std::ptrdiff_t offset = ptr_val - base_val;
        if (offset % sizeof(Block) != 0) {
            return;
        }

        std::size_t index = offset / sizeof(Block);
        if (index >= BlockCount) {
            return;
        }

        std::size_t current_head = next_free_index_.load(std::memory_order_relaxed);
        do {
            *reinterpret_cast<std::size_t*>(&free_list_storage_[index * kCacheLineSize]) = current_head;
        } while (!next_free_index_.compare_exchange_weak(current_head, index, 
                                                         std::memory_order_release, 
                                                         std::memory_order_relaxed));
    }

    [[nodiscard]] static constexpr std::size_t block_size() noexcept { return BlockSize; }
    [[nodiscard]] static constexpr std::size_t block_count() noexcept { return BlockCount; }
    [[nodiscard]] static constexpr std::size_t capacity_bytes() noexcept { return BlockSize * BlockCount; }

private:
    static constexpr std::size_t kInvalidIndex = static_cast<std::size_t>(-1);

    alignas(kCacheLineSize) std::array<Block, BlockCount> blocks_;
    // Intrusive singly-linked free list stored directly within unused blocks or parallel metadata cachelines
    alignas(kCacheLineSize) std::array<uint8_t, BlockCount * kCacheLineSize> free_list_storage_;
    alignas(kCacheLineSize) std::atomic<std::size_t> next_free_index_;
};

// 2. CIRCULAR RING BUFFER (SPSCRingBuffer): Lock-free, power-of-two bounded queue with explicit cacheline padding
template <typename T, std::size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
public:
    SPSCRingBuffer() noexcept 
        : head_(0), tail_(0) {}

    ~SPSCRingBuffer() = default;

    SPSCRingBuffer(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer(SPSCRingBuffer&&) noexcept = delete;
    SPSCRingBuffer& operator=(SPSCRingBuffer&&) noexcept = delete;

    [[nodiscard]] bool push(const T& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Ring buffer full (triggers backpressure / drop policy)
        }

        buffer_[current_tail & kMask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool push(T&& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false;
        }

        buffer_[current_tail & kMask] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool pop(T& item) noexcept {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false; // Ring buffer empty
        }

        item = std::move(buffer_[current_head & kMask]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t current_head = head_.load(std::memory_order_relaxed);
        std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        if (current_tail >= current_head) {
            return current_tail - current_head;
        }
        return 0;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept { return Capacity; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    alignas(kCacheLineSize) std::atomic<std::size_t> head_;
    alignas(kCacheLineSize) std::atomic<std::size_t> tail_;
    alignas(kCacheLineSize) std::array<T, Capacity> buffer_;
};

// 3. SLIDING-WINDOW FLOW CONTROL: Time-bucketed bitmap and sequence tracking arrays for O(1) mutations
template <std::size_t WindowSizeBits = 1024, std::size_t BucketCount = 64>
class SlidingWindowFlowController {
    static_assert((WindowSizeBits % 64) == 0, "WindowSizeBits must be a multiple of 64 for word alignment");
public:
    struct alignas(kCacheLineSize) TimeBucket {
        std::atomic<uint64_t> bitmap[WindowSizeBits / 64];
        std::atomic<uint64_t> epoch_timestamp;

        void reset(uint64_t epoch) noexcept {
            epoch_timestamp.store(epoch, std::memory_order_relaxed);
            for (auto& word : bitmap) {
                word.store(0, std::memory_order_relaxed);
            }
        }
    };

    SlidingWindowFlowController() noexcept 
        : current_bucket_index_(0) {
        uint64_t initial_epoch = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
        for (auto& bucket : buckets_) {
            bucket.reset(initial_epoch);
        }
    }

    ~SlidingWindowFlowController() = default;

    SlidingWindowFlowController(const SlidingWindowFlowController&) = delete;
    SlidingWindowFlowController& operator=(const SlidingWindowFlowController&) = delete;
    SlidingWindowFlowController(SlidingWindowFlowController&&) noexcept = delete;
    SlidingWindowFlowController& operator=(SlidingWindowFlowController&&) noexcept = delete;

    [[nodiscard]] bool record_and_check(uint64_t sequence_id, uint64_t current_epoch_ms) noexcept {
        std::size_t bucket_idx = (current_epoch_ms / 100) % BucketCount;
        auto& bucket = buckets_[bucket_idx];

        uint64_t stored_epoch = bucket.epoch_timestamp.load(std::memory_order_relaxed);
        if (stored_epoch != (current_epoch_ms / 100)) {
            // Epoch shift detected: perform lazy bucket reclamation / reset
            if (bucket.epoch_timestamp.compare_exchange_strong(stored_epoch, (current_epoch_ms / 100), 
                                                               std::memory_order_release, 
                                                               std::memory_order_relaxed)) {
                for (auto& word : bucket.bitmap) {
                    word.store(0, std::memory_order_relaxed);
                }
            }
        }

        std::size_t bit_offset = sequence_id % WindowSizeBits;
        std::size_t word_idx = bit_offset / 64;
        uint64_t bit_mask = 1ULL << (bit_offset % 64);

        uint64_t old_word = bucket.bitmap[word_idx].fetch_or(bit_mask, std::memory_order_acq_rel);
        
        // Return true if already set (duplicate / replay packet detected)
        return (old_word & bit_mask) != 0;
    }

    void reset_bucket(std::size_t bucket_idx, uint64_t epoch) noexcept {
        if (bucket_idx < BucketCount) {
            buckets_[bucket_idx].reset(epoch);
        }
    }

private:
    alignas(kCacheLineSize) std::array<TimeBucket, BucketCount> buckets_;
    alignas(kCacheLineSize) std::atomic<std::size_t> current_bucket_index_;
};

// Zero-copy packet descriptor wrapper maintaining absolute alignment and zero allocation semantics
struct alignas(kCacheLineSize) PacketDescriptor {
    uint8_t* payload_ptr;
    uint32_t length;
    uint32_t flow_hash;
    uint64_t timestamp_ns;

    PacketDescriptor() noexcept
        : payload_ptr(nullptr), length(0), flow_hash(0), timestamp_ns(0) {}

    PacketDescriptor(uint8_t* ptr, uint32_t len, uint32_t hash, uint64_t ts) noexcept
        : payload_ptr(ptr), length(len), flow_hash(hash), timestamp_ns(ts) {}

    [[nodiscard]] bool valid() const noexcept {
        return payload_ptr != nullptr && length > 0;
    }
};

} // namespace titan

#endif // TITAN_TYPES_HPP