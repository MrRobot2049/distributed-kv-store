#pragma once

#include <optional>
#include <string>
#include <unordered_map>

namespace raftkv {

class KeyValueStateMachine {
 public:
    virtual ~KeyValueStateMachine() = default;

    virtual void Put(std::string key, std::string value) = 0;
    virtual std::optional<std::string> Get(const std::string& key) const = 0;
    virtual void Delete(const std::string& key) = 0;
};

class StateMachine final : public KeyValueStateMachine {
 public:
    void Put(std::string key, std::string value) override;
    std::optional<std::string> Get(const std::string& key) const override;
    void Delete(const std::string& key) override;

 private:
    std::unordered_map<std::string, std::string> data_;
};

}  // namespace raftkv
