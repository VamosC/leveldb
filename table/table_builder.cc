// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  std::string last_key;
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  // 这里的Rep* r仅仅是为了名字的简单性
  // 写代码更快 看起来更简洁罢了
  // 有点下述代码的感觉 比如
  // int nidsandandasndasdasda;
  // int &r = nidsandandasndasdasda;
  Rep* r = rep_;
  assert(!r->closed);
  // r->status.ok
  // rep_->status.ok
  if (!ok()) return;
  // r->num_entries > 0
  // 否则没有办法比较
  if (r->num_entries > 0) {
    // 下一个key肯定要比上一个key更“大”
    // 否则说明系统出现了错误 因为memtable必定是sorted的
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // init = false
  // 进入的时候应该是下一个block
  // 此时pending_index_entry是true
  // 建立这个索引都是开到了下一个block的第一个key才干的
  // lookached(1)
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 为了压缩索引值的长度
    // 比如 whodasdasa 与whoeadsdads
    // 完全可以用whoe来隔开
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // offset是上一个block的起点 size是相对应的内容的长度
    // 全部填入handle_encoding中
    r->pending_handle.EncodeTo(&handle_encoding);
    // 加入index中方便索引寻找
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 有filter机制
  // add key
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 记录last_key
  // last_key是上一个key的意思
  r->last_key.assign(key.data(), key.size());
  // 数量+1
  r->num_entries++;
  // 将key和value加入datablock中
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  // 数据的大小与用户设置的block的大小相比较
  // 决定是否刷入kernel区域, 进而进入disk
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

// 已分析
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  // datablock肯定是非空的
  // 否则直接推出即可
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  // rep_->status.ok
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  // 建立filter机制
  // 具体看filter 特别是bloom filter
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

// 压缩方式待分析
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // datablock
  // 得到要写的内容
  // shared nonshared valuesize + nonshared的key + value
  // 最后+start数组+start数组的size
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  // 压缩的方式
  switch (type) {
    case kNoCompression:
      // 不压缩就直接赋值
      block_contents = raw;
      break;

      // 待分析!!
    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  WriteRawBlock(block_contents, type, handle);
  // 压缩后的输出的清理
  r->compressed_output.clear();
  // 重置block
  // 修改finish标志位
  block->Reset();
}

// 已分析
// 最后加上1byte type 以及 4bytes crc checksum
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // 设置偏移
  handle->set_offset(r->offset);
  // 设置大小
  handle->set_size(block_contents.size());
  // 内容写入.ldb文件
  r->status = r->file->Append(block_contents);
  // 写入成功的话
  if (r->status.ok()) {
    // 1byte type + 4byte checksum(crc)
    char trailer[kBlockTrailerSize];
    // 这里的type代表的是 是否被压缩过
    trailer[0] = type;
    // 获得crc
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    // Append接受的是Slice
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    // 更新offset
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

// 已分析
// 返回当前的状态
Status TableBuilder::status() const { return rep_->status; }

// 不仅要将之前未达到阈值而没有写入的data写入
// 还要带上index等附加的加速查找用的metadata(元数据)
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 将当前未写完的全部写入
  Flush();
  assert(!r->closed);
  // 设置close标志
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

// 已分析
// 返回数量
uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
