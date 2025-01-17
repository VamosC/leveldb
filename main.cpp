#include <cassert>
#include "leveldb/db.h"

int main() {
  leveldb::DB* db;
  leveldb::Options options;
  options.error_if_exists = true;
  leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
  assert(status.ok());
}