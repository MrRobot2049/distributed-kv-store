#pragma once

#include <cstdint>
#include <string>

#include "common/types.h"

namespace raftkv {

struct RaftLogEntry {
    Term term{0};
    LogIndex index{0};
    std::string command;
};

}  // namespace raftkv
