#ifndef TITAN_PACKET_FILTER_HPP
#define TITAN_PACKET_FILTER_HPP

#include <cstdint>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <new>
#include <atomic>
#include <chrono>
#include <system_error>
#include <limits>

namespace titan {

constexpr std::size_able_t = std::size_t;

enum class FilterResult : uint8_t {
    ACCEPT = 0,
    DROP_POLICY = 1,
    DROP_ARENA_EXHAUSTION = 2,
    DROP_RING_FULL = 3
};

struct alignas(64) PacketMetadata {
    uint64_t timestamp_ns;
    uint32_t packet_id;
    uint32_t length;
    uint32_t flow_hash;
    FilterResult result;
    uint8_t reserved[35];
};

static_assert(sizeof(PacketMetadata) == 64, "PacketMetadata must be exactly 64 bytes for cache alignment.");

class ArenaAllocator {
public:
    explicit ArenaAllocator(std::size_t block_size, std::size_t block_count)
        : block_size_(block_size),
          block_count_(block_count),
          total_size_(block_size_ * block_count_),
          arena_memory_(nullptr),
          free_list_(nullptr),
          allocated_blocks_(0) {
        
        if (block_size_ < 64) {
            block_size_ = 64;
        }
        
        // Align block size to 64 bytes
        block_size_ = (block_size_ + 63) & ~static_std_size_t(63);
        total_size_ = block_size_ * block_count_;

        int alloc_result = posix_memalign(&arena_memory_, 64, total_size_);
        if (alloc_result != 0 || arena_memory_ == nullptr) {
            throw std::system_error(alloc_result, std::generic_category(), "Failed to allocate cache-aligned arena memory");
        }

        std::memset(arena_memory_, 0, total_size_);

        // Initialize free list
        struct FreeNode {
            FreeNode* next;
        };

        uint8_t* cursor = static_cast<uint8_t*>(arena_memory_);
        for (std::size_t i = 0; i < block_count_ - 1; ++i) {
            FreeNode* current = reinterpret_cast<FreeNode*>(cursor);
            FreeNode* next_node = reinterpret_cast<FreeNode*>(cursor + block_size_);
            current->next = next_node;
            cursor += block_size_;
        }
        reinterpret_cast<FreeNode*>(cursor)->next = nullptr;
        free_list_ = arena_memory_;
    }

    ~ArenaAllocator() {
        if (arena_memory_ != nullptr) {
            std::free(arena_memory_);
            arena_memory_ = nullptr;
        }
    }

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;

    ArenaAllocator(ArenaAllocator&& other) noexcept
        : block_size_(other.block_size_),
          block_count_(other.block_count_),
          total_size_(other.total_size_),
          arena_memory_(other.arena_memory_),
          free_list_(other.free_list_),
          allocated_blocks_(other.allocated_blocks_.load(std::memory_order_relaxed)) {
        other.arena_memory_ = nullptr;
        other.free_list_ = nullptr;
        other.allocated_blocks_ = 0;
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            if (arena_memory_ != nullptr) {
                std::free(arena_memory_);
            }
            block_size_ = other.block_size_;
            block_count_ = other.block_count_;
            total_size_ = other.total_size_;
            arena_memory_ = other.arena_memory_;
            free_list_ = other.free_list_;
            allocated_blocks_.store(other.allocated_blocks_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            
            other.arena_memory_ = nullptr;
            other.free_list_ = nullptr;
            other.allocated_blocks_ = 0;
        }
        return *this;
    }

    void* allocate() noexcept {
        struct FreeNode {
            FreeNode* next;
        };

        void* ptr = nullptr;
        // Simple lock-free single-producer/multi-consumer style extraction using internal pointer logic
        // For a strict thread-safe arena without mutexes, we manage the head via atomic CAS if shared, 
        // but here we use a simple CAS loop for thread safety.
        
        FreeNode* old_head = static_cast<FreeNode*>(free_list_.load(std::memory_order_acquire));
        while (old_head != nullptr) {
            FreeNode* new_head = old_head->next;
            if (free_list_.compare_exchange_weak(old_head, new_head, 
                                                 std::memory_order_release, 
                                                 std::memory_order_relaxed)) {
                ptr = static_cast<void*>(old_head);
                allocated_blocks_.fetch_add(1, std::memory_order_relaxed);
                break;
            }
        }
        return ptr;
    }

    void deallocate(void* ptr) noexcept {
        if (ptr == nullptr) {
            return;
        }

        struct FreeNode {
            FreeNode* next;
        };

        FreeNode* node = static_cast<FreeNode*>(ptr);
        FreeNode* old_head = static_cast<FreeNode*>(free_list_.load(std::memory_order_relaxed));
        do {
            node->next = old_head;
        } while (!free_list_.compare_exchange_weak(old_head, node, 
                                                   std::memory_order_release, 
                                                   std::memory_order_relaxed));
        
        allocated_blocks_.fetch_sub(1, std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t block_size() const noexcept { return block_size_; }
    [[nodiscard]] std::size_t block_count() const noexcept { return block_count_; }
    [[nodiscard]] std::size_t allocated_count() const noexcept { 
        return allocated_blocks_.load(std::memory_order_relaxed); 
    }
    [[nodiscard]] bool exhausted() const noexcept {
        return allocated_blocks_.load(std::memory_order_relaxed) >= block_count_;
    }

    void force_reclaim_stale() noexcept {
        // Reset arena completely under emergency reclamation
        uint8_t* cursor = static_cast<uint8_t*>(arena_memory_);
        struct FreeNode {
            FreeNode* next;
        };

        for (std::size_t i = 0; i < block_count_ - 1; ++i) {
            FreeNode* current = reinterpret_cast<FreeNode*>(cursor);
            FreeNode* next_node = reinterpret_cast<FreeNode*>(cursor + block_size_);
            current->next = next_node;
            cursor += block_size_;
        }
        reinterpret_cast<FreeNode*>(cursor)->next = nullptr;
        free_list_.store(arena_memory_, std::memory_order_release);
        allocated_blocks_.store(0, std::memory_order_release);
    }

private:
    std::size_t block_size_;
    std::size_t block_count_;
    std::size_t total_size_;
    void* arena_memory_;
    std::atomic<void*> free_list_;
    std::atomic<std::size_t> allocated_blocks_;

    std::size_t static_std_size_t(std::size_t val) { return val; }
};

template <typename T, std::size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    SPSCRingBuffer() : head_(0), tail_(0) {
        for (std::size_t i = 0; i < Capacity; ++i) {
            buffer_[i] = T();
        }
    }

    ~SPSCRingBuffer() = default;

    SPSCRingBuffer(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;

    bool push(const T& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Ring full
        }

        buffer_[current_tail & kMask] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(T&& item) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Ring full
        }

        buffer_[current_tail & kMask] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool pop(T& item) noexcept {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false; // Ring empty
        }

        item = std::move(buffer_[current_head & kMask]);
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail >= head) {
            return tail - head;
        }
        return 0;
    }

    [[nodiscard]] std::size_t capacity() const noexcept {
        return Capacity;
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
    alignas(64) T buffer_[Capacity];
};

class SlidingWindowFlowControl {
public:
    explicit SlidingWindowFlowControl(uint32_t window_buckets = 64)
        : window_buckets_(window_buckets),
          bucket_mask_(window_buckets - 1),
          current_bucket_index_(0),
          last_bucket_timestamp_(0) {
        if ((window_buckets_ & (window_buckets_ - 1)) != 0) {
            // Force power of two if not compliant
            window_buckets_ = 64;
            bucket_mask_ = 63;
        }
        
        drop_bitmap_ = new std::atomic<uint64_t>[window_buckets_];
        accept_bitmap_ = new std::atomic<uint64_t>[window_buckets_];

        for (uint32_t i = 0; i < window_buckets_; ++i) {
            drop_bitmap_[i].store(0, std::memory_order_relaxed);
            accept_bitmap_[i].store(0, std::memory_order_relaxed);
        }
    }

    ~SlidingWindowFlowControl() {
        delete[] drop_bitmap_;
        delete[] accept_bitmap_;
    }

    SlidingWindowFlowControl(const SlidingWindowFlowControl&) = delete;
    SlidingWindowFlowControl& operator=(const SlidingWindowFlowControl&) = delete;

    void record_event(uint64_t timestamp_ns, FilterResult result) noexcept {
        uint64_t bucket_time_frame = timestamp_ns / 1000000ULL; // Millisecond buckets
        uint32_t target_bucket = static_cast<uint32_t>(bucket_time_frame & bucket_mask_);

        uint32_t current_idx = current_bucket_index_.load(std::memory_order_relaxed);
        if (target_bucket != current_idx) {
            if (current_bucket_index_.compare_exchange_strong(current_idx, target_bucket, std::memory_order_relaxed)) {
                // Clear out stale bucket metrics for sliding window progression
                drop_bitmap_[target_bucket].store(0, std::memory_order_release);
                accept_bitmap_[target_bucket].store(0, std::memory_order_release);
            }
        }

        if (result == FilterResult::ACCEPT) {
            atomic_increment_bitmap(accept_bitmap_[target_bucket]);
        } else {
            atomic_increment_bitmap(drop_bitmap_[target_bucket]);
        }
    }

    [[nodiscard]] uint64_t get_total_drops() const noexcept {
        uint64_t total = 0;
        for (uint32_t i = 0; i < window_buckets_; ++i) {
            total += drop_bitmap_[i].load(std::memory_order_relaxed);
        }
        return total;
    }

    [[nodiscard]] uint64_t get_total_accepts() const noexcept {
        uint64_t total = 0;
        for (uint32_t i = 0; i < window_buckets_; ++i) {
            total += accept_bitmap_[i].load(std::memory_order_relaxed);
        }
        return total;
    }

    [[nodiscard]] double get_drop_rate() const noexcept {
        uint64_t drops = get_total_drops();
        uint64_t accepts = get_total_accepts();
        uint64_t total = drops + accepts;
        if (total == 0) {
            return 0.0;
        }
        return static_cast<double>(drops) / static_cast<double>(total);
    }

private:
    void atomic_increment_bitmap(std::atomic<uint64_t>& counter) noexcept {
        uint64_t current = counter.load(std::memory_order_relaxed);
        while (!counter.compare_exchange_weak(current, current + 1,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
            // Retry loop for lockfree atomic increment
        }
    }

    uint32_t window_buckets_;
    uint32_t bucket_mask_;
    std::atomic<uint32_t> current_bucket_index_;
    uint64_t last_bucket_timestamp_;
    std::atomic<uint64_t>* drop_bitmap_;
    std::atomic<uint64_t>* accept_bitmap_;
};

class PacketFilterEngine {
public:
    PacketFilterEngine(std::size_t block_size, std::size_t block_count, std::size_t ring_capacity)
        : arena_(block_size, block_count),
          ring_buffer_(),
          flow_control_(64),
          packet_counter_(0),
          red_threshold_(0.85) {}

    ~PacketFilterEngine() = default;

    PacketFilterEngine(const PacketFilterEngine&) = delete;
    PacketFilterEngine& operator=(const PacketFilterEngine&) = delete;

    [[nodiscard]] FilterResult evaluate_and_ingest(const uint8_t* payload, uint32_t length, uint32_t flow_hash) noexcept {
        uint64_t now = static_cast<uint6