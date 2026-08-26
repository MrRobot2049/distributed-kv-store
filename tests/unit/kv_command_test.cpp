#include "storage/kv_command.h"

#include <gtest/gtest.h>

#include "storage/state_machine.h"

namespace raftkv {
namespace {

TEST(KvCommandTest, EncodesAndDecodesPut) {
    const auto decoded = DecodeCommand(EncodePutCommand("key with spaces", "value bytes"));

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->type, KvCommandType::Put);
    EXPECT_EQ(decoded->key, "key with spaces");
    EXPECT_EQ(decoded->value, "value bytes");
}

TEST(KvCommandTest, EncodesAndDecodesDelete) {
    const auto decoded = DecodeCommand(EncodeDeleteCommand("old-key"));

    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->type, KvCommandType::Delete);
    EXPECT_EQ(decoded->key, "old-key");
}

TEST(KvCommandTest, RejectsMalformedCommands) {
    EXPECT_EQ(DecodeCommand(""), std::nullopt);
    EXPECT_EQ(DecodeCommand(std::string(1, '\x7f')), std::nullopt);
}

TEST(KvCommandTest, AppliesDecodedCommands) {
    StateMachine state_machine;
    const auto put = DecodeCommand(EncodePutCommand("color", "blue"));
    ASSERT_TRUE(put.has_value());

    EXPECT_TRUE(ApplyCommand(*put, state_machine));
    EXPECT_EQ(state_machine.Get("color"), std::optional<std::string>("blue"));

    const auto delete_command = DecodeCommand(EncodeDeleteCommand("color"));
    ASSERT_TRUE(delete_command.has_value());

    EXPECT_TRUE(ApplyCommand(*delete_command, state_machine));
    EXPECT_EQ(state_machine.Get("color"), std::nullopt);
}

}  // namespace
}  // namespace raftkv
