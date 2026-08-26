#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "common/log_entry.h"
#include "common/types.h"
#include "raft/log_manager.h"
#include "raft/raft_metadata_store.h"
#include "storage/state_machine.h"

namespace raftkv {

enum class KvCommandType {
    Put,
    Delete,
};

struct KvCommand {
    KvCommandType type;
    std::string key;
    std::string value;
};

std::string EncodePutCommand(const std::string& key, const std::string& value);
std::string EncodeDeleteCommand(const std::string& key);
std::optional<KvCommand> DecodeCommand(std::string_view encoded);

bool ApplyCommand(const KvCommand& command, KeyValueStateMachine& state_machine);
LogIndex ReplayCommittedEntries(RaftLog& log, RaftMetadataStore& metadata_store,
                                KeyValueStateMachine& state_machine);

}  // namespace raftkv
