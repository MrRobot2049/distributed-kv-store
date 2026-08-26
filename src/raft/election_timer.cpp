#include "raft/election_timer.h"

namespace raftkv {

ElectionTimer::ElectionTimer(std::chrono::milliseconds min_timeout,
                             std::chrono::milliseconds max_timeout,
                             std::uint32_t seed)
    : rng_(seed),
      timeout_distribution_(static_cast<int>(min_timeout.count()),
                            static_cast<int>(max_timeout.count())) {}

std::chrono::milliseconds ElectionTimer::NextTimeout() {
    return std::chrono::milliseconds(timeout_distribution_(rng_));
}

}  // namespace raftkv
