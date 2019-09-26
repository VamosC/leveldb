// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/filename.h"

#include <ctype.h>
#include <stdio.h>

#include "db/dbformat.h"
#include "leveldb/env.h"
#include "util/logging.h"

namespace leveldb {

// A utility routine: write "data" to the named file and Sync() it.
// 具体实现见env.cc
// 将data写入 file 并同步也就是刷新至文件
// 已分析
Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname);

// dbname/000001.suffix
// 已分析
static std::string MakeFileName(const std::string& dbname, uint64_t number,
                                const char* suffix) {
  char buf[100];
  snprintf(buf, sizeof(buf), "/%06llu.%s",
           static_cast<unsigned long long>(number), suffix);
  return dbname + buf;
}

// dbname/000001.log
// 已分析
std::string LogFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "log");
}

// dbname/000001.ldb
// 已分析
std::string TableFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "ldb");
}

// dbname/000001.sst
// 已分析
std::string SSTTableFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  return MakeFileName(dbname, number, "sst");
}

// dbname/MANIFEST-000001
// dbname/MANIFEST-%06llu
// 已分析
std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  char buf[100];
  // (1) 如果格式化后的字符串长度 < size,
  // 则将此字符串全部复制到str中,并给其后添加一个字符串结束符('\0');
  // (2) 如果格式化后的字符串长度 >= size,则只将其中的(size - 1)
  // 个字符复制到str中,并给其后添加一个字符串结束符('\0')
  // 返回值为欲写入的字符串长度
  snprintf(buf, sizeof(buf), "/MANIFEST-%06llu",
           static_cast<unsigned long long>(number));
  return dbname + buf;
}

// dbname/CURRENT
// 已分析
std::string CurrentFileName(const std::string& dbname) {
  return dbname + "/CURRENT";
}

// dbname/LOCK
// 已分析
std::string LockFileName(const std::string& dbname) { return dbname + "/LOCK"; }

// dbname/000001.dbtmp
// dbname/%06llu.dbtmp
// 已分析
std::string TempFileName(const std::string& dbname, uint64_t number) {
  assert(number > 0);
  // "dbtmp"是后缀名
  return MakeFileName(dbname, number, "dbtmp");
}

// dbname/LOG
// 已分析
std::string InfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG";
}

// dbname/LOG.old
// 已分析
// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname) {
  return dbname + "/LOG.old";
}

// Owned filenames have the form:
//    dbname/CURRENT
//    dbname/LOCK
//    dbname/LOG
//    dbname/LOG.old
//    dbname/MANIFEST-[0-9]+
//    dbname/[0-9]+.(log|sst|ldb)
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type) {
  Slice rest(filename);
  if (rest == "CURRENT") {
    *number = 0;
    *type = kCurrentFile;
  } else if (rest == "LOCK") {
    *number = 0;
    *type = kDBLockFile;
  } else if (rest == "LOG" || rest == "LOG.old") {
    *number = 0;
    *type = kInfoLogFile;
  } else if (rest.starts_with("MANIFEST-")) {
    rest.remove_prefix(strlen("MANIFEST-"));
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    if (!rest.empty()) {
      return false;
    }
    *type = kDescriptorFile;
    *number = num;
  } else {
    // Avoid strtoull() to keep filename format independent of the
    // current locale
    uint64_t num;
    if (!ConsumeDecimalNumber(&rest, &num)) {
      return false;
    }
    Slice suffix = rest;
    if (suffix == Slice(".log")) {
      *type = kLogFile;
    } else if (suffix == Slice(".sst") || suffix == Slice(".ldb")) {
      *type = kTableFile;
    } else if (suffix == Slice(".dbtmp")) {
      *type = kTempFile;
    } else {
      return false;
    }
    *number = num;
  }
  return true;
}

// 将ManiFest的文件名写入Current
// 已分析
Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number) {
  // Remove leading "dbname/" and add newline to manifest file name
  // dbname/MANIFEST-000001
  std::string manifest = DescriptorFileName(dbname, descriptor_number);
  Slice contents = manifest;
  // 断言检查是否在dbname/目录下
  assert(contents.starts_with(dbname + "/"));
  // 删除前缀 仅仅保留文件名称
  contents.remove_prefix(dbname.size() + 1);

  // dbname/000001.dbtmp
  std::string tmp = TempFileName(dbname, descriptor_number);
  Status s = WriteStringToFileSync(env, contents.ToString() + "\n", tmp);
  if (s.ok()) {
    // dbname/CURRENT
    // std::rename(from, to);
    s = env->RenameFile(tmp, CurrentFileName(dbname));
  }
  if (!s.ok()) {
    // unlink
    env->DeleteFile(tmp);
  }
  return s;
}

}  // namespace leveldb
