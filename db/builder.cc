// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

// iter => memtable::iterator => skiplist::iterator
// options = dpimpl->options_
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  // 寻找第一个(key, value)
  iter->SeekToFirst();

  // e.g. dbname/000001.ldb
  std::string fname = TableFileName(dbname, meta->number);
  // 对memtable的iterator的所有调用都会转化为skiplist中iterator的调用
  // 判断第一个(key, value)是否为空
  // 换句话说判断memtable是否为空 只有一个虚表头
  if (iter->Valid()) {
    WritableFile* file;
    // 创建writablefile
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    // get key 存放在InternalKey中
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      // auto key = iter->key();
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      // bulider建立并且适时地写入file中
      builder->Add(key, iter->value());
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      // 同步file
      s = file->Sync();
    }
    if (s.ok()) {
      // 关闭file
      s = file->Close();
    }
    // delete file也是关闭file假如s.ok() == false
    delete file;
    // file置为nullptr是优秀的习惯
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  // 文件长度 > 0否则没有意义
  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    // unlink()
    // <=> linux command: rm -f fname
    env->DeleteFile(fname);
  }
  return s;
}

}  // namespace leveldb
