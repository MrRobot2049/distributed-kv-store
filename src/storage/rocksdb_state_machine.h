#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "storage/state_machine.h"

namespace raftkv {

class RocksDbStateMachine final : public KeyValueStateMachine {
 public:
    explicit RocksDbStateMachine(std::filesystem::path db_path);
    ~RocksDbStateMachine();

    RocksDbStateMachine(const RocksDbStateMachine&) = delete;
    RocksDbStateMachine& operator=(const RocksDbStateMachine&) = delete;

    void Put(std::string key, std::string value) override;
    std::optional<std::string> Get(const std::string& key) const override;
    void Delete(const std::string& key) override;

 private:
    rocksdb::ColumnFamilyHandle* KvDataCf() const;

    std::filesystem::path db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    std::vector<rocksdb::ColumnFamilyHandle*> column_families_;
};

}  // namespace raftkv
