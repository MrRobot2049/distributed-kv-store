#include "storage/kv_command.h"

#include <cstdint>
#include <limits>
#include <string_view>

namespace raftkv {
namespace {

constexpr char kPutCommand = 1;
constexpr char kDeleteCommand = 2;

void AppendUint32(std::string* output, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        output->push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

std::optional<std::uint32_t> ReadUint32(std::string_view input, std::size_t* offset) {
    if (input.size() - *offset < sizeof(std::uint32_t)) {
        return std::nullopt;
    }

    std::uint32_t value = 0;
    for (std::size_t i = 0; i < sizeof(std::uint32_t); ++i) {
        value <<= 8;
        value |= static_cast<unsigned char>(input[(*offset)++]);
    }

    return value;
}

bool AppendLengthPrefixed(std::string* output, const std::string& value) {
    if (value.size() > std::numeric_limits<std::uint32_t>::max()) {
        return false;
    }

    AppendUint32(output, static_cast<std::uint32_t>(value.size()));
    output->append(value);
    return true;
}

std::optional<std::string> ReadLengthPrefixed(std::string_view input, std::size_t* offset) {
    const auto size = ReadUint32(input, offset);
    if (!size.has_value() || input.size() - *offset < *size) {
        return std::nullopt;
    }

    std::string value(input.substr(*offset, *size));
    *offset += *size;
    return value;
}

}  // namespace

std::string EncodePutCommand(const std::string& key, const std::string& value) {
    std::string encoded;
    encoded.push_back(kPutCommand);
    if (!AppendLengthPrefixed(&encoded, key) || !AppendLengthPrefixed(&encoded, value)) {
        return {};
    }

    return encoded;
}

std::string EncodeDeleteCommand(const std::string& key) {
    std::string encoded;
    encoded.push_back(kDeleteCommand);
    if (!AppendLengthPrefixed(&encoded, key)) {
        return {};
    }

    return encoded;
}

std::optional<KvCommand> DecodeCommand(std::string_view encoded) {
    if (encoded.empty()) {
        return std::nullopt;
    }

    std::size_t offset = 1;
    const char command_type = encoded[0];
    auto key = ReadLengthPrefixed(encoded, &offset);
    if (!key.has_value()) {
        return std::nullopt;
    }

    if (command_type == kPutCommand) {
        auto value = ReadLengthPrefixed(encoded, &offset);
        if (!value.has_value() || offset != encoded.size()) {
            return std::nullopt;
        }

        return KvCommand{.type = KvCommandType::Put, .key = *key, .value = *value};
    }

    if (command_type == kDeleteCommand && offset == encoded.size()) {
        return KvCommand{.type = KvCommandType::Delete, .key = *key};
    }

    return std::nullopt;
}

bool ApplyCommand(const KvCommand& command, KeyValueStateMachine& state_machine) {
    switch (command.type) {
        case KvCommandType::Put:
            state_machine.Put(command.key, command.value);
            return true;
        case KvCommandType::Delete:
            state_machine.Delete(command.key);
            return true;
    }

    return false;
}

LogIndex ReplayCommittedEntries(RaftLog& log, RaftMetadataStore& metadata_store,
                                KeyValueStateMachine& state_machine) {
    const LogIndex commit_index = metadata_store.CommitIndex();
    LogIndex last_applied = metadata_store.LastApplied();
    for (LogIndex index = last_applied + 1; index <= commit_index; ++index) {
        const auto entries = log.EntriesFrom(index);
        if (entries.empty() || entries.front().index != index) {
            break;
        }

        const auto command = DecodeCommand(entries.front().command);
        if (!command.has_value() || !ApplyCommand(*command, state_machine)) {
            break;
        }

        last_applied = index;
        metadata_store.SaveLastApplied(last_applied);
    }

    return last_applied;
}

}  // namespace raftkv
