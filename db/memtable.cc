// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include "db/skiplist.h"
#include "port/cache_flush.h"
#include <cstdio>
#include <gnuwrapper.h>
#include <string>
#include <unordered_set>


namespace leveldb {

static Slice GetLengthPrefixedSlice(const char* data) {
    uint32_t len;
    const char* p = data;
    p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
    return Slice(p, len);
}

void MemTable::AddPredictIndex
                (std::unordered_set<std::string> *set,
                        const uint8_t* data) {
    this->bloom_.add(data, strlen((const char*)data));
}

int MemTable::CheckPredictIndex
            (std::unordered_set<std::string> *set,
                    const uint8_t* data) {
    return this->bloom_.possiblyContains(data,
            (size_t)strlen((const char*)data));
}

//TODO: Implement prediction clear
void MemTable::ClearPredictIndex(std::unordered_set<std::string> *set) {
}

void* MemTable::operator new(std::size_t sz) {
    return malloc(sz);
}

void* MemTable::operator new[](std::size_t sz) {
    return malloc(sz);
}
void MemTable::operator delete(void* ptr)
{
    free(ptr);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
: comparator_(cmp),
  refs_(0),
  logfile_number(0),
  numkeys_(0),
  bloom_(BLOOMSIZE, BLOOMHASH),
  table_(comparator_, &arena_),
  sub_imm_skiplist(comparator_, &arena_) {
    sub_mem_skiplist = new Table[arena_.sub_mem_count](comparator_, &arena_);
    sub_mem_pending_node_index = (int*)malloc(sizeof(int) * arena_.sub_mem_count);
    sub_mem_pending_node = new std::vector<char*>[arena_.sub_mem_count];
    isQueBusy.store(0);

    for(int i=0; i<arena_.sub_mem_count; i++) {
        sub_mem_pending_node_index[i] = 0;
    }
}

MemTable::MemTable(const InternalKeyComparator& cmp, ArenaNVM& arena, bool recovery)
: comparator_(cmp),
  refs_(0),
  logfile_number(0),
  arena_(arena),
  numkeys_(0),
  bloom_(BLOOMSIZE, BLOOMHASH),
  table_(comparator_, &arena_, recovery),
  sub_imm_skiplist(comparator_, &arena_, recovery) {
    arena_.nvmarena_ = arena.nvmarena_;
    sub_mem_skiplist = new Table[arena_.sub_mem_count](comparator_, &arena_, recovery);
    sub_mem_pending_node_index = (int*)malloc(sizeof(int) * arena_.sub_mem_count);
    sub_mem_pending_node = new std::vector<char*>[arena_.sub_mem_count];
    isQueBusy.store(0);

    for(int i=0; i<arena_.sub_mem_count; i++) {
        sub_mem_pending_node_index[i] = 0;
    }
}

MemTable::~MemTable() {
    assert(refs_ == 0);
    delete[] sub_mem_skiplist;
    free(sub_mem_pending_node_index);
    delete[] sub_mem_pending_node;
}


size_t MemTable::ApproximateMemoryUsage() 
{
    if(this->isNVMMemtable == true) {
        ArenaNVM *nvm_arena = (ArenaNVM *)&arena_;
        return nvm_arena->MemoryUsage();
    }
    return arena_.MemoryUsage();
}

//size_t MemTable::ApproximateArenaMemoryUsage() { return arena_.MemoryUsage(); }
int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
const {
    // Internal keys are encoded as length-prefixed strings.
    Slice a = GetLengthPrefixedSlice(aptr);
    Slice b = GetLengthPrefixedSlice(bptr);
    return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char* EncodeKey(std::string* scratch, const Slice& target) {
    scratch->clear();
    PutVarint32(scratch, target.size());
    scratch->append(target.data(), target.size());
    return scratch->data();
}

class MemTableIterator: public Iterator {
public:
    explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

    virtual bool Valid() const { return iter_.Valid(); }
    virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); }
    virtual void SeekToFirst() { iter_.SeekToFirst(); }
    virtual void SeekToLast() { iter_.SeekToLast(); }
    virtual void Next() { iter_.Next(); }
    virtual void Prev() { iter_.Prev(); }

#ifdef USE_OFFSETS
    virtual char *GetNodeKey(){
        return reinterpret_cast<char *>((intptr_t)iter_.key_offset());
    }
#else
    virtual char *GetNodeKey(){return iter_.key(); }
#endif

#if defined(USE_OFFSETS)
    virtual Slice key() const { return GetLengthPrefixedSlice(reinterpret_cast<const char *>((intptr_t)iter_.key_offset())); }
#else
    virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
#endif
    virtual Slice value() const {
#if defined(USE_OFFSETS)
        Slice key_slice = GetLengthPrefixedSlice(reinterpret_cast<const char *>((intptr_t)iter_.key_offset()));
#else
        Slice key_slice = GetLengthPrefixedSlice(iter_.key());
#endif
        return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
    }
    //NoveLSM
    //virtual void SetHead(void *ptr) { iter_.SetHead(ptr); }
    void* operator new(std::size_t sz) {
        return malloc(sz);
    }

    void* operator new[](std::size_t sz) {
        return malloc(sz);
    }
    void operator delete(void* ptr)
    {
        free(ptr);
    }
    virtual Status status() const { return Status::OK(); }

private:
    MemTable::Table::Iterator iter_;
    std::string tmp_;       // For passing to EncodeKey

    // No copying allowed
    MemTableIterator(const MemTableIterator&);
    void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
    return new MemTableIterator(&table_);
}

Iterator* MemTable::NewSubMemIterator(int index){
	return new MemTableIterator(&sub_mem_skiplist[index]);
}


void MemTable::SetMemTableHead(void *ptr){
    //table_.SetHead(ptr);
    //table_.head_ = (Node *)ptr;
    table_.SetHead(ptr);
}


void* MemTable::GeTableoffset(){
    return table_.head_offset_;
}

void MemTable::Add(SequenceNumber s, ValueType type,
        const Slice& key,
        const Slice& value) {
    // Format of an entry is concatenation of:
    //  key_size     : varint32 of internal_key.size()
    //  key bytes    : char[internal_key.size()]
    //  value_size   : varint32 of value.size()
    //  value bytes  : char[value.size()]
    size_t key_size = key.size();
    size_t val_size = value.size();
    size_t internal_key_size = key_size + 8;
    const size_t encoded_len =
            VarintLength(internal_key_size) + internal_key_size +
            VarintLength(val_size) + val_size;
    char* buf = NULL;
retry:
    ArenaNVM *nvm_arena = (ArenaNVM *)&arena_;
    if(arena_.nvmarena_) {
        buf = nvm_arena->Allocate(encoded_len);
    }else {
        buf = arena_.Allocate(encoded_len);
    }
    if(!buf){
        //usleep(70000); 
        //nvm_arena->reclaim_sub_mem(-1);
        goto retry;
        perror("Memory allocation failed");
        exit(-1);
    }

    char* p = EncodeVarint32(buf, internal_key_size);

    //TODO: Disabling the STM transaction library in this beta
    //Some performance issues if cores are not rightly pinned
    //to NUMA nodes. Simply adding the memory copy persist
    //Will be re-enabled in next version soon.
    if (this->isNVMMemtable == true) {
        memcpy(p, key.data(), key_size);
    }else{
        memcpy(p, key.data(), key_size);
    }

#ifdef _ENABLE_PREDICTION
    char *keystr = (char*)key.data();
    keystr[key_size]=0;
    AddPredictIndex(&predict_set, (const char *)keystr);
#endif

    p += key_size;
    EncodeFixed64(p, (s << 8) | type);
    p += 8;
    p = EncodeVarint32(p, val_size);

    if (this->isNVMMemtable == true) {
          memcpy(p, value.data(), val_size);
    }else{
          memcpy(p, value.data(), val_size);
    }
    assert((p + val_size) - buf == encoded_len);

    int sub_mem_index;
    if(arena_.nvmarena_) {
        ArenaNVM *nvm_arena = (ArenaNVM *)&arena_;
        sub_mem_index = (buf - (char*)nvm_arena->map_start_) / SUB_MEM_SIZE;
    }else {
        sub_mem_index = (buf - (char*)arena_.map_start_) / SUB_MEM_SIZE;
    }
    sub_mem_pending_node[sub_mem_index].push_back(buf);
/*
#ifdef ENABLE_RECOVERY
    table_.Insert(buf, s);
    sub_mem_skiplist[sub_mem_index].Insert(buf, s);
#else
    table_.Insert(buf);
    sub_mem_skiplist[sub_mem_index].Insert(buf);
#endif
*/
    //NoveLSM: We keep track of the number of keys inserted
    //into each memtable
    //TODO (OPT): Using a macro?
    this->IncrKeys();
}


bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {

    Slice memkey = key.memtable_key();
    Table::Iterator iter(&table_);
    iter.Seek(memkey.data());
    if (iter.Valid()) {
        // entry format is:
        //    klength  varint32
        //    userkey  char[klength]
        //    tag      uint64
        //    vlength  varint32
        //    value    char[vlength]
        // Check that it belongs to same user key.  We do not check the
        // sequence number since the Seek() call above should have skipped
        // all entries with overly large sequence numbers.
#if defined(USE_OFFSETS)
        const char* entry = reinterpret_cast<const char *>((intptr_t)iter.key_offset());
#else
        const char* entry = iter.key();
#endif
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
        if (comparator_.comparator.user_comparator()->Compare(
                Slice(key_ptr, key_length - 8),
                key.user_key()) == 0) {
            // Correct user key
            const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
            switch (static_cast<ValueType>(tag & 0xff)) {
            case kTypeValue: {
                Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                value->assign(v.data(), v.size());
                return true;
            }
            case kTypeDeletion:
                *s = Status::NotFound(Slice());
                return true;
            }
        }
    }
    return false;
}

bool MemTable::Get_submem(const LookupKey& key, std::string* value, Status* s){
    Slice memkey = key.memtable_key();
    Table::Iterator iter(&table_);

    for(int i=0; i<arena_.sub_mem_count; i++){
        if(!arena_.sub_mem_bset[i].load() || !arena_.sub_immem_bset[i].load())
            continue;
        iter.list_ = &sub_mem_skiplist[i];
        iter.node_ = NULL;
        iter.Seek(memkey.data());
        if(iter.Valid())
            break;
    }

    if (iter.Valid()) {
#if defined(USE_OFFSETS)
        const char* entry = reinterpret_cast<const char *>((intptr_t)iter.key_offset());
#else
        const char* entry = iter.key();
#endif
        uint32_t key_length;
        const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);
        if (comparator_.comparator.user_comparator()->Compare(
                Slice(key_ptr, key_length - 8),
                key.user_key()) == 0) {
            // Correct user key
            const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
            switch (static_cast<ValueType>(tag & 0xff)) {
            case kTypeValue: {
                Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
                value->assign(v.data(), v.size());
                return true;
            }
            case kTypeDeletion:
                *s = Status::NotFound(Slice());
                return true;
            }
        }
    }
    return false;
}


}  // namespace leveldb
