#ifndef TITAN_RING_BUFFER_HPP
#define TITAN_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <type_traits>

namespace titan {

template <typename T, std::size_t Capacity>
class SPSCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
    static_assert(Capacity > 0, "Capacity must be greater than zero.");

public:
    SPSCRingBuffer() 
        : head_(0), tail_(0) {
        storage_ = static_cast<Node*>(std::aligned_alloc(64, sizeof(Node) * Capacity));
        if (!storage_) {
            throw std::runtime_error("SPSCRingBuffer: Failed to allocate aligned memory for storage.");
        }
        for (std::size_t i = 0; i < Capacity; ++i) {
            new (&storage_[i]) Node();
        }
    }

    ~SPSCRingBuffer() {
        if (storage_) {
            for (std::size_t i = 0; i < Capacity; ++i) {
                storage_[i].~Node();
            }
            std::free(storage_);
            storage_ = nullptr;
        }
    }

    SPSCRingBuffer(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer& operator=(const SPSCRingBuffer&) = delete;
    SPSCRingBuffer(SPSCRingBuffer&&) = delete;
    SPSCRingBuffer& operator=(SPSCRingBuffer&&) = delete;

    template <typename... Args>
    bool emplace(Args&&... args) {
        const std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        const std::size_t current_head = head_.load(std::memory_order_acquire);

        if ((current_tail - current_head) >= Capacity) {
            return false; // Buffer full
        }

        Node* node = &storage_[current_tail & kMask];
        new (&node->storage) T(std::forward<Args>(args)...);
        node->initialized = true;

        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    bool push(const T& item) {
        return emplace(item);
    }

    bool push(T&& item) {
        return emplace(std::move(item));
    }

    bool pop(T& item) {
        const std::size_t current_head = head_.load(std::memory_order_relaxed);
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);

        if (current_head == current_tail) {
            return false; // Buffer empty
        }

        Node* node = &storage_[current_head & kMask];
        if (!node->initialized) {
            return false;
        }

        item = std::move(*reinterpret_cast<T*>(&node->storage));
        reinterpret_cast<T*>(&node->storage)->~T();
        node->initialized = false;

        head_.store(current_head + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    [[nodiscard]] bool full() const noexcept {
        const std::size_t current_tail = tail_.load(std::memory_order_acquire);
        const std::size_t current_head = head_.load(std::memory_order_acquire);
        return (current_tail - current_head) >= Capacity;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t current_head = head_.load(std::memory_order_acquire);
        std::size_t current_tail = tail_.load(std::memory_order_acquire);
        if (current_tail >= current_head) {
            return current_tail - current_head;
        }
        return 0;
    }

    [[nodiscard]] static constexpr std::size_t capacity() noexcept {
        return Capacity;
    }

private:
    struct alignas(64) Node {
        typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
        bool initialized{false};
    };

    static constexpr std::size_t kMask = Capacity - 1;

    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
    Node* storage_{nullptr};

    char pad_[64 - sizeof(Node*)] = {0};
};

} // namespace titan

#endif // TITAN_RING_BUFFER_HPP