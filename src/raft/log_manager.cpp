#include "raft/log_manager.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace raftkv {

void LogManager::Append(RaftLogEntry entry) {
    if (entry.index == 0) {
        throw std::invalid_argument("log entry indexes are 1-based");
    }

    if (entry.index <= LastIndex()) {
        TruncateFrom(entry.index);
    }

    const LogIndex expected_index = entries_.empty() ? base_index_ + 1 : LastIndex() + 1;
    if (entry.index != expected_index) {
        throw std::invalid_argument("log entry index must be contiguous");
    }

    entries_.push_back(std::move(entry));
}

bool LogManager::MatchesAt(LogIndex index, Term term) const {
    if (index == 0) {
        return term == 0;
    }

    const auto it = std::find_if(entries_.begin(), entries_.end(), [index](const auto& entry) {
        return entry.index == index;
    });
    return it != entries_.end() && it->term == term;
}

void LogManager::TruncateFrom(LogIndex index) {
    if (index == 0) {
        entries_.clear();
        return;
    }

    const auto erase_begin =
        std::find_if(entries_.begin(), entries_.end(), [index](const auto& entry) {
            return entry.index >= index;
        });
    entries_.erase(erase_begin, entries_.end());
}

void LogManager::CompactBefore(LogIndex index) {
    base_index_ = std::max(base_index_, index);
    if (index <= 1) {
        return;
    }

    const auto erase_end =
        std::find_if(entries_.begin(), entries_.end(), [index](const auto& entry) {
            return entry.index >= index;
        });
    entries_.erase(entries_.begin(), erase_end);
}

void LogManager::SetBaseIndex(LogIndex index) {
    base_index_ = std::max(base_index_, index);
}

std::vector<RaftLogEntry> LogManager::EntriesFrom(LogIndex index) const {
    if (index == 0) {
        index = 1;
    }

    const auto begin = std::find_if(entries_.begin(), entries_.end(), [index](const auto& entry) {
        return entry.index >= index;
    });
    return {begin, entries_.end()};
}

LogIndex LogManager::LastIndex() const {
    if (entries_.empty()) {
        return base_index_;
    }

    return entries_.back().index;
}

Term LogManager::TermAt(LogIndex index) const {
    if (index == 0) {
        return 0;
    }

    const auto it = std::find_if(entries_.begin(), entries_.end(), [index](const auto& entry) {
        return entry.index == index;
    });
    if (it == entries_.end()) {
        throw std::out_of_range("log index is not present");
    }

    return it->term;
}

}  // namespace raftkv
