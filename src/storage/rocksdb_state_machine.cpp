#include "storage/rocksdb_state_machine.h"

#include <stdexcept>
#include <string_view>
#include <utility>

#include <rocksdb/options.h>

namespace raftkv {
namespace {

constexpr std::string_view kMetadataColumnFamily = "metadata";
constexpr std::string_view kRaftLogColumnFamily = "raft_log";
constexpr std::string_view kKvDataColumnFamily = "kv_data";

void ThrowIfNotOk(const rocksdb::Status& status, std::string_view operation) {
    if (!status.ok()) {
        throw std::runtime_error(std::string(operation) + ": " + status.ToString());
    }
}

std::vector<rocksdb::ColumnFamilyDescriptor> StoreColumnFamilies() {
    return {
        rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName,
                                        rocksdb::ColumnFamilyOptions()),
        rocksdb::ColumnFamilyDescriptor(std::string(kMetadataColumnFamily),
                                        rocksdb::ColumnFamilyOptions()),
        rocksdb::ColumnFamilyDescriptor(std::string(kRaftLogColumnFamily),
                                        rocksdb::ColumnFamilyOptions()),
        rocksdb::ColumnFamilyDescriptor(std::string(kKvDataColumnFamily),
                                        rocksdb::ColumnFamilyOptions()),
    };
}

}  // namespace

RocksDbStateMachine::RocksDbStateMachine(std::filesystem::path db_path)
    : db_path_(std::move(db_path)) {
    if (db_path_.has_parent_path()) {
        std::filesystem::create_directories(db_path_.parent_path());
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    ThrowIfNotOk(rocksdb::DB::Open(options, db_path_.string(), StoreColumnFamilies(),
                                   &column_families_, &db_),
                 "open rocksdb state machine");
}

RocksDbStateMachine::~RocksDbStateMachine() {
    for (auto* column_family : column_families_) {
        if (column_family != nullptr) {
            db_->DestroyColumnFamilyHandle(column_family);
        }
    }
}

void RocksDbStateMachine::Put(std::string key, std::string value) {
    rocksdb::WriteOptions options;
    options.sync = true;
    ThrowIfNotOk(db_->Put(options, KvDataCf(), key, value), "put kv state");
}

std::optional<std::string> RocksDbStateMachine::Get(const std::string& key) const {
    std::string value;
    const rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), KvDataCf(), key, &value);
    if (status.IsNotFound()) {
        return std::nullopt;
    }

    ThrowIfNotOk(status, "get kv state");
    return value;
}

void RocksDbStateMachine::Delete(const std::string& key) {
    rocksdb::WriteOptions options;
    options.sync = true;
    ThrowIfNotOk(db_->Delete(options, KvDataCf(), key), "delete kv state");
}

rocksdb::ColumnFamilyHandle* RocksDbStateMachine::KvDataCf() const {
    return column_families_.at(3);
}

}  // namespace raftkv
