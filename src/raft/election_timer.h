#pragma once

#include <chrono>
#include <random>

namespace raftkv {

class ElectionTimer {
 public:
    ElectionTimer(std::chrono::milliseconds min_timeout,
                  std::chrono::milliseconds max_timeout,
                  std::uint32_t seed = std::random_device{}());

    std::chrono::milliseconds NextTimeout();

 private:
    std::mt19937 rng_;
    std::uniform_int_distribution<int> timeout_distribution_;
};

}  // namespace raftkv
