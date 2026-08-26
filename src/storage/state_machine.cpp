#include "storage/state_machine.h"

#include <utility>

namespace raftkv {

void StateMachine::Put(std::string key, std::string value) {
    data_[std::move(key)] = std::move(value);
}

std::optional<std::string> StateMachine::Get(const std::string& key) const {
    const auto it = data_.find(key);
    if (it == data_.end()) {
        return std::nullopt;
    }

    return it->second;
}

void StateMachine::Delete(const std::string& key) {
    data_.erase(key);
}

}  // namespace raftkv
