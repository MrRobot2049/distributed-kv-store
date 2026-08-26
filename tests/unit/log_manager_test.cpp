#include "raft/log_manager.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace raftkv {
namespace {

TEST(LogManagerTest, AppendsContiguousEntries) {
    LogManager log;

    log.Append(RaftLogEntry{.term = 1, .index = 1, .command = "put a 1"});
    log.Append(RaftLogEntry{.term = 1, .index = 2, .command = "put b 2"});

    EXPECT_EQ(log.LastIndex(), 2);
    EXPECT_TRUE(log.MatchesAt(2, 1));
    EXPECT_EQ(log.TermAt(1), 1);
}

TEST(LogManagerTest, RejectsGaps) {
    LogManager log;
    log.Append(RaftLogEntry{.term = 1, .index = 1, .command = "put a 1"});

    EXPECT_THROW(log.Append(RaftLogEntry{.term = 1, .index = 3, .command = "put c 3"}),
                 std::invalid_argument);
}

TEST(LogManagerTest, ConflictingAppendTruncatesExistingSuffix) {
    LogManager log;
    log.Append(RaftLogEntry{.term = 1, .index = 1, .command = "put a 1"});
    log.Append(RaftLogEntry{.term = 1, .index = 2, .command = "put b 2"});
    log.Append(RaftLogEntry{.term = 2, .index = 2, .command = "put b 3"});

    EXPECT_EQ(log.LastIndex(), 2);
    EXPECT_EQ(log.TermAt(2), 2);
    EXPECT_EQ(log.EntriesFrom(2).front().command, "put b 3");
}

TEST(LogManagerTest, EntriesFromReturnsSuffix) {
    LogManager log;
    log.Append(RaftLogEntry{.term = 1, .index = 1, .command = "a"});
    log.Append(RaftLogEntry{.term = 2, .index = 2, .command = "b"});
    log.Append(RaftLogEntry{.term = 2, .index = 3, .command = "c"});

    const auto entries = log.EntriesFrom(2);

    ASSERT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].index, 2);
    EXPECT_EQ(entries[1].index, 3);
}

TEST(LogManagerTest, IndexZeroIsTheEmptyLogSentinel) {
    LogManager log;

    EXPECT_TRUE(log.MatchesAt(0, 0));
    EXPECT_EQ(log.TermAt(0), 0);
    EXPECT_TRUE(log.EntriesFrom(0).empty());
}

}  // namespace
}  // namespace raftkv
