#include "storage/rocksdb_state_machine.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace raftkv {
namespace {

std::filesystem::path TempDbPath(const std::string& test_name) {
    return std::filesystem::temp_directory_path() / ("raftkv_state_" + test_name);
}

class RocksDbStateMachineTest : public testing::Test {
 protected:
    void SetUp() override {
        db_path_ = TempDbPath(testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(db_path_);
    }

    void TearDown() override {
        std::filesystem::remove_all(db_path_);
    }

    std::filesystem::path db_path_;
};

TEST_F(RocksDbStateMachineTest, PutGetAndDeletePersistKvData) {
    {
        RocksDbStateMachine state_machine(db_path_);
        state_machine.Put("color", "blue");
        EXPECT_EQ(state_machine.Get("color"), std::optional<std::string>("blue"));
        state_machine.Delete("color");
        EXPECT_EQ(state_machine.Get("color"), std::nullopt);
        state_machine.Put("shape", "circle");
    }

    RocksDbStateMachine reopened(db_path_);
    EXPECT_EQ(reopened.Get("shape"), std::optional<std::string>("circle"));
    EXPECT_EQ(reopened.Get("color"), std::nullopt);
}

}  // namespace
}  // namespace raftkv
