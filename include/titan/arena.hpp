#ifndef TITAN_ARENA_HPP
#define TITAN_ARENA_HPP

#include <cstdint>
#include <cstddef>
#include <new>
#include <atomic>
#include <array>
#include <system_error>
#include <type_traits>

namespace titan {

constexpr std::size_t CACHE_LINE_SIZE = 64;

enum class ArenaError {
    Success = 0,
    OutOfMemory,
    InvalidAlignment,
    BufferOverflow
};

class ArenaErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "titan_arena";
    }

    std::string message(int ev) const override {
        switch (static_cast<ArenaError>(ev)) {
            case ArenaError::Success: return "Success";
            case ArenaError::OutOfMemory: return "Arena out of memory";
            case ArenaError::InvalidAlignment: return "Invalid alignment requested";
            case ArenaError::BufferOverflow: return "Buffer overflow detected";
            default: return "Unknown arena error";
        }
    }
};

inline const std::error_category& get_arena_error_category() noexcept {
    static ArenaErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ArenaError e) noexcept {
    return {static_cast<int>(e), get_arena_error_category()};
}

} // namespace titan

namespace std {
template <>
struct is_error_code_enum<titan::ArenaError> : true_type {};
} // namespace std

namespace titan {

template <std::size_t CapacityBytes>
class alignas(CACHE_LINE_SIZE) ArenaAllocator {
private:
    alignas(CACHE_LINE_SIZE) std::array<std::byte, CapacityBytes> storage_;
    std::atomic<std::size_t> offset_{0};

    static constexpr std::size_t align_forward(std::size_t addr, std::size_t alignment) noexcept {
        return (addr + (alignment - 1)) & ~(alignment - 1);
    }

public:
    ArenaAllocator() noexcept = default;
    ~ArenaAllocator() noexcept = default;

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;
    ArenaAllocator(ArenaAllocator&&) noexcept = delete;
    ArenaAllocator& operator=(ArenaAllocator&&) noexcept = delete;

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        constexpr std::size_t size = sizeof(T);
        constexpr std::size_t alignment = alignof(T) > CACHE_LINE_SIZE ? alignof(T) : CACHE_LINE_SIZE;

        std::size_t current_offset = offset_.load(std::memory_order_relaxed);
        std::size_t aligned_offset = 0;
        std::size_t next_offset = 0;

        do {
            aligned_offset = align_forward(current_offset, alignment);
            next_offset = aligned_offset + size;
            if (next_offset > CapacityBytes) {
                return nullptr;
            }
        } while (!offset_.compare_exchange_weak(
            current_offset,
            next_offset,
            std::memory_order_release,
            std::memory_order_relaxed));

        void* ptr = static_cast<void*>(storage_.data() + aligned_offset);
        return ::new (ptr) T(std::forward<Args>(args)...);
    }

    void* allocate_bytes(std::size_t size, std::size_t alignment = CACHE_LINE_SIZE) noexcept {
        if (alignment < CACHE_LINE_SIZE) {
            alignment = CACHE_LINE_SIZE;
        }

        std::size_t current_offset = offset_.load(std::memory_order_relaxed);
        std::size_t aligned_offset = 0;
        std::size_t next_offset = 0;

        do {
            aligned_offset = align_forward(current_offset, alignment);
            next_offset = aligned_offset + size;
            if (next_offset > CapacityBytes) {
                return nullptr;
            }
        } while (!offset_.compare_exchange_weak(
            current_offset,
            next_offset,
            std::memory_order_release,
            std::memory_order_relaxed));

        return static_cast<void*>(storage_.data() + aligned_offset);
    }

    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
    }

    [[nodiscard]] std::size_t used_bytes() const noexcept {
        return offset_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t available_bytes() const noexcept {
        std::size_t current = offset_.load(std::memory_order_relaxed);
        return current <= CapacityBytes ? CapacityBytes - current : 0;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return CapacityBytes;
    }
};

template <typename T, std::size_t Capacity>
class alignas(CACHE_LINE_SIZE) SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");

private:
    alignas(CACHE_LINE_SIZE) std::array<T, Capacity> buffer_;
    
    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> head_{0};
    alignas(CACHE_LINE_SIZE) std::atomic<std::size_t> tail_{0};

    static constexpr std::size_t MASK = Capacity - 1;

public:
    SPSCRingBuffer() noexcept = default;
    ~SPSCRingBuffer() noexcept = default;

    SPSCRingBuffer(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer(SPSCRingBuffer&&) noexcept = delete;
    SPSCRingBuffer& operator=(SPSCRingBuffer&&) noexcept = delete;

    template <typename... Args>
    bool emplace(Args&&... args) noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) == Capacity) {
            return false;
        }

        new (&buffer_[current_tail & MASK]) T(std::forward<Args>(args)...);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(const T& item) noexcept {
        return emplace(item);
    }

    bool push(T&& item) noexcept {
        return emplace(std::move(item));
    }

    bool pop(T& item) noexcept {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false;
        }

        item = std::move(buffer_[current_head & MASK]);
        buffer_[current_head & MASK].~T();
        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool full() const noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);
        return (current_tail - current_head) == Capacity;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t current_tail = tail_.load(std::memory_order_acquire);
        std::size_t current_head = head_.load(std::memory_order_acquire);
        if (current_tail >= current_head) {
            return current_tail - current_head;
        }
        return 0;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }
};

} // namespace titan

#endif // TITAN_ARENA_HPP