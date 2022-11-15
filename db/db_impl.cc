
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#define __STDC_FORMAT_MACROS

#include "db/db_impl.h"

#include <algorithm>
#include <set>
#include <string>
#include <stdint.h>
#include <stdio.h>
#include <vector>
#include <iostream>
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/debug.h"
#include "hoard/heaplayers/wrappers/gnuwrapper.h"
#include "util/thpool.h"
#include <inttypes.h>
#include <string>
#include <unordered_set>
#include <chrono>
#include <ctime>

#include <pqos.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

namespace leveldb {
std::atomic_int subImmCount;

const int kNumNonTableCacheFiles = 10;
bool kCheckCond = 0;
uint64_t numreqsts=0;
uint64_t numhits=0;
int num_read_threads=0;
int knvmhit = 0;

#ifdef _ENABLE_PREDICTION
bool predict_on = true;
#else
bool predict_on = false;
#endif

/* TODO:NoveLSM global variables
 * Requries cleanup
 */
MemTable *g_imm;
MemTable *g_mem;
const leveldb::ReadOptions g_options;
bool mem_found = false;
bool sstable_found = false;
bool imm_found = false;
//Foreground compaction time
std::chrono::duration<double> fgcompactime;
uint64_t mem_hits=0;
uint64_t imm_hits=0;
uint64_t sstable_hits = 0;


// Information kept for every waiting writer
struct DBImpl::Writer {
    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;

    explicit Writer(port::Mutex* mu) : cv(mu) { }
};

/* Methods for stats purpose
 * ToDO: Moving states to a new
 * file
 */
void inline incr_mem_hits(void){
    mem_hits++;
}

void inline incr_sstable_hits(void){
    sstable_hits++;
}

void inline incr_imm_hits(void){
    imm_hits++;
}


struct DBImpl::CompactionState {
    Compaction* const compaction;

    // Sequence numbers < smallest_snapshot are not significant since we
    // will never have to service a snapshot below smallest_snapshot.
    // Therefore if we have seen a sequence number S <= smallest_snapshot,
    // we can drop all entries for the same key with sequence numbers < S.
    SequenceNumber smallest_snapshot;

    // Files produced by compaction
    struct Output {
        uint64_t number;
        uint64_t file_size;
        InternalKey smallest, largest;
    };
    std::vector<Output> outputs;

    // State kept for output being generated
    WritableFile* outfile;
    TableBuilder* builder;

    uint64_t total_bytes;

    Output* current_output() { return &outputs[outputs.size()-1]; }

    explicit CompactionState(Compaction* c)
    : compaction(c),
      outfile(NULL),
      builder(NULL),
      total_bytes(0) {
    }
};

// Fix user-supplied options to be reasonable
template <class T,class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
    if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
    if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
Options SanitizeOptions(const std::string& dbname,
        const InternalKeyComparator* icmp,
        const InternalFilterPolicy* ipolicy,
        const Options& src) {
    Options result = src;
    result.comparator = icmp;
    result.filter_policy = (src.filter_policy != NULL) ? ipolicy : NULL;
    ClipToRange(&result.max_open_files,    64 + kNumNonTableCacheFiles, 50000);
    ClipToRange(&result.write_buffer_size, 64<<10,                      1<<30);
    //NoveLSM write_buffer_size_fix. Remove the line if all tests succeed
    //ClipToRange(&result.nvm_buffer_size, 64<<10,                      1<<30);
    ClipToRange(&result.block_size,        1<<10,                       4<<20);
    if (result.info_log == NULL) {
        // Open a log file in the same directory as the db
        src.env->CreateDir(dbname);  // In case it does not exist
        src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
        Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);
        if (!s.ok()) {
            // No place suitable for logging
            result.info_log = NULL;
        }
    }
    if (result.block_cache == NULL) {
        result.block_cache = NewLRUCache(8 << 20);
    }
    return result;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname_disk, const std::string& dbname_mem)
: env_(raw_options.env),
  internal_comparator_(raw_options.comparator),
  internal_filter_policy_(raw_options.filter_policy),
  options_(SanitizeOptions(dbname_disk, &internal_comparator_,
          &internal_filter_policy_, raw_options)),
          owns_info_log_(options_.info_log != raw_options.info_log),
          owns_cache_(options_.block_cache != raw_options.block_cache),
          dbname_disk_(dbname_disk),
          dbname_mem_(dbname_mem),
          db_lock_(NULL),
          shutting_down_(NULL),
          bg_cv_(&mutex_),
          mem_(NULL),
          imm_(NULL),
          use_multiple_levels(true),
          logfile_(NULL),
          /*NoveLSM: Map number for mmap file */
          mapfile_number_(0),
          logfile_number_(0),
          log_(NULL),
          seed_(0),
          tmp_batch_(new WriteBatch),
          bg_compaction_scheduled_(false),
          manual_compaction_(NULL) {
    subImmKill = 0;
    isFirstArena = 1;
    inSkiplistBgSync.store(0);
    inCompactImm.store(0);
    skiplistSync_threshold = options_.skiplistSync_threshold;
    compactImm_threshold = options_.compactImm_threshold;
    subImm_partition = options_.subImm_partition;
    subImm_thread = options_.subImm_thread;

    has_imm_.Release_Store(NULL);

    /*NoveLSM specific parameters*/
    num_read_threads = raw_options.num_read_threads;
    drambuff_ = options_.write_buffer_size;
    nvmbuff_ = options_.nvm_buffer_size;

    // Reserve ten files or so for other uses and give the rest to TableCache.
    const int table_cache_size = options_.max_open_files - kNumNonTableCacheFiles;
    DEBUG_T("dbname_disk_ %s, dbname_mem_ %s \n",dbname_disk_.c_str(), dbname_mem_.c_str());
    table_cache_ = new TableCache(dbname_disk_, &options_, table_cache_size);

    //VersionSet uses dbname to place and locate MANIFEST and CURRENT files, which reside in disk for now
    versions_ = new VersionSet(dbname_disk_, &options_, table_cache_,
            &internal_comparator_);
    thpool = NULL;
}

DBImpl::~DBImpl() {
    ArenaNVM *tmp_arena = reinterpret_cast<ArenaNVM*>(&mem_->arena_);
    tmp_arena->setSubMemToImm();
    skiplistBackgroundSync(this);
    compactImm(this);
    env_->SleepForMicroseconds(100000);
#ifdef _ENABLE_STATS
    std::cout << "Foreground compaction time: " << fgcompactime.count() << "s\n";
    fprintf(stderr,"mem_hits %u, imm_hits %u sstable_hits %u\n",
            mem_hits, imm_hits, sstable_hits);
#endif
    // Wait for background work to finish
    mutex_.Lock();
    shutting_down_.Release_Store(this);  // Any non-NULL value is ok
    while (bg_compaction_scheduled_) {
        bg_cv_.Wait();
    }
    mutex_.Unlock();

    if (db_lock_ != NULL) {
        env_->UnlockFile(db_lock_);
    }

    delete versions_;
    if (mem_ != NULL) mem_->Unref();
    if (imm_ != NULL) imm_->Unref();
    delete tmp_batch_;
    delete log_;
    delete logfile_;
    delete table_cache_;

    if (owns_info_log_) {
        delete options_.info_log;
    }
    if (owns_cache_) {
        delete options_.block_cache;
    }

    if(thpool && NUM_READ_THREADS)
        thpool_destroy(thpool);
}

Status DBImpl::NewDB() {
    VersionEdit new_db;
    new_db.SetComparatorName(user_comparator()->Name());
    new_db.SetLogNumber(0);
    /* NoveLSM: Initialize memtable map file to 1*/
    new_db.SetMapNumber(1);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::string manifest = DescriptorFileName(dbname_disk_, 1);
    WritableFile* file;
    Status s = env_->NewWritableFile(manifest, &file);
    if (!s.ok()) {
        return s;
    }

    log::Writer log(file);
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);
    if (s.ok()) {
        s = file->Close();
    }
    delete file;
    if (s.ok()) {
        // Make "CURRENT" file that points to the new manifest file.
        s = SetCurrentFile(env_, dbname_disk_, 1);
    } else {
        env_->DeleteFile(manifest);
    }
    return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
    if (s->ok() || options_.paranoid_checks) {
        // No change needed
    } else {
        Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
        *s = Status::OK();
    }
}

void DBImpl::DeleteObsoleteFiles() {
    if (!bg_error_.ok()) {
        // After a background error, we don't know whether a new version may
        // or may not have been committed, so we cannot safely garbage collect.
        return;
    }

    // Make a set of all of the live files
    std::set<uint64_t> live = pending_outputs_;
    versions_->AddLiveFiles(&live);

    std::vector<std::string> filenames;
    std::vector<std::string> filenames_mem;
    env_->GetChildren(dbname_disk_, &filenames); // Ignoring errors on purpose
    if (dbname_disk_ != dbname_mem_) {
        env_->GetChildren(dbname_mem_, &filenames_mem); // Ignoring errors on purpose
        filenames.insert(filenames.end(), filenames_mem.begin(), filenames_mem.end());
    }
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            bool keep = true;
            switch (type) {
            case kLogFile:
                keep = ((number >= versions_->LogNumber()) ||
                        (number == versions_->PrevLogNumber()));
                break;
                //NoveLSM: NVM memtable skip list recovery changes
            case kMapFile:
                keep = (number >= versions_->MapNumber());
                break;
            case kDescriptorFile:
                // Keep my manifest file, and any newer incarnations'
                // (in case there is a race that allows other incarnations)
                keep = (number >= versions_->ManifestFileNumber());
                break;
            case kTableFile:
                keep = (live.find(number) != live.end());
                break;
            case kTempFile:
                // Any temp files that are currently being written to must
                // be recorded in pending_outputs_, which is inserted into "live"
                keep = (live.find(number) != live.end());
                break;
            case kCurrentFile:
            case kDBLockFile:
            case kInfoLogFile:
                keep = true;
                break;
            }

            if (!keep) {
                if (type == kTableFile) {
                    table_cache_->Evict(number);
                }
                Log(options_.info_log, "Delete type=%d #%lld\n",
                        int(type),
                        static_cast<unsigned long long>(number));
                if (find(filenames_mem.begin(), filenames_mem.end(), filenames[i]) != filenames_mem.end())
                    env_->DeleteFile(dbname_mem_ + "/" + filenames[i]);
                else
                    env_->DeleteFile(dbname_disk_ + "/" + filenames[i]);
            }
        }
    }
}


Status DBImpl::Recover(VersionEdit* edit, bool *save_manifest) {
    mutex_.AssertHeld();

    // Ignore error from CreateDir since the creation of the DB is
    // committed only when the descriptor is created, and this directory
    // may already exist from a previous failed creation attempt.
    env_->CreateDir(dbname_disk_);
    env_->CreateDir(dbname_mem_);
    assert(db_lock_ == NULL);
    Status s = env_->LockFile(LockFileName(dbname_disk_), &db_lock_);
    if (!s.ok()) {
        return s;
    }

    if (!env_->FileExists(CurrentFileName(dbname_disk_))) {
        if (options_.create_if_missing) {
            s = NewDB();
            if (!s.ok()) {
                return s;
            }
        } else {
            return Status::InvalidArgument(
                    dbname_disk_, "does not exist (create_if_missing is false)");
        }
    } else {
        if (options_.error_if_exists) {
            return Status::InvalidArgument(
                    dbname_disk_, "exists (error_if_exists is true)");
        }
    }

    s = versions_->Recover(save_manifest);
    if (!s.ok()) {
        return s;
    }
    SequenceNumber max_sequence(0);

    // Recover from all newer log files than the ones named in the
    // descriptor (new log files may have been added by the previous
    // incarnation without registering them in the descriptor).
    //
    // Note that PrevLogNumber() is no longer used, but we pay
    // attention to it in case we are recovering a database
    // produced by an older version of leveldb.
    const uint64_t min_log = versions_->LogNumber();

    //NoveLSM:Get the minimum NVM memtable map number
    const uint64_t min_map = versions_->MapNumber();

    const uint64_t prev_log = versions_->PrevLogNumber();
    std::vector<std::string> filenames;
    std::vector<std::string> filenames_mem;
    s = env_->GetChildren(dbname_disk_, &filenames);
    if (!s.ok()) {
        return s;
    }

    //Recovering from the NVM Mem path. The Map files are stored here.
    if (dbname_disk_ != dbname_mem_) {
        s = env_->GetChildren(dbname_mem_, &filenames_mem);
        if (!s.ok()) {
            return s;
        }
        filenames.insert(filenames.end(), filenames_mem.begin(), filenames_mem.end());
    }

    std::set<uint64_t> expected;
    versions_->AddLiveFiles(&expected);
    uint64_t number;
    FileType type;
    std::vector<uint64_t> logs;
    std::vector<uint64_t> maps;
    for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type)) {
            expected.erase(number);
            if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
                logs.push_back(number);
            /* NoveLSM: NVM memtable map file */
            else if (type == kMapFile && number >= min_map) {
                maps.push_back(number);
                DEBUG_T("Map number recovery %llu \n", number);
            }
        }
    }
    if (!expected.empty()) {
        char buf[50];
        snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                static_cast<int>(expected.size()));
        return Status::Corruption(buf, TableFileName(dbname_disk_, *(expected.begin())));
    }

    // Recover in the order in which the logs were generated
    std::sort(logs.begin(), logs.end());
    std::sort(maps.begin(), maps.end());
    if (maps.size() > 0 || logs.size() > 0) {

        if (maps.size() == 0) {
            RecoverLogFile(logs[0], true, save_manifest, edit, &max_sequence);
            versions_->MarkFileNumberUsed(logs[0]);
            //NoveLSM: Set the NVM memtable map file with incrementing
            //log number
            mapfile_number_ = logfile_number_;
        }
        else if (logs.size() == 0) {
            //NoveLSM: There is only one log file and it is a NVM memtable
            // Iterate and recover
            mapfile_number_ = maps[0];
            for (std::vector<uint64_t>::iterator it = maps.begin(); it != maps.end(); ++it) {
                uint64_t map_num = *it;
                RecoverMapFile(map_num, save_manifest, edit, &max_sequence);
                versions_->MarkFileNumberUsed(map_num);
            }
        }
        else if (logs[0] > maps[0]) {
            //NoveLSM: The first DRAM log is newwer than NVM memtable file
            //Recover incrementally
            mapfile_number_ = maps[0];
            for (std::vector<uint64_t>::iterator it = maps.begin(); it != maps.end(); ++it) {
                uint64_t map_num = *it;
                RecoverMapFile(map_num, save_manifest, edit, &max_sequence);
                versions_->MarkFileNumberUsed(map_num);
            }
            RecoverLogFile(logs[0], true, save_manifest, edit, &max_sequence);
            versions_->MarkFileNumberUsed(logs[0]);
        }
        else {
            RecoverLogFile(logs[0], true, save_manifest, edit, &max_sequence);
            versions_->MarkFileNumberUsed(logs[0]);

            //NoveLSM: TODO: Try to make all these blocks of NVM map file recovery
            //into one block 	
            mapfile_number_ = maps[0];
            for (std::vector<uint64_t>::iterator it = maps.begin(); it != maps.end(); ++it) {
                uint64_t map_num = *it;
                RecoverMapFile(map_num, save_manifest, edit, &max_sequence);
                versions_->MarkFileNumberUsed(map_num);
            }
        }
    }

    if (versions_->LastSequence() < max_sequence) {
        versions_->SetLastSequence(max_sequence);
    }

    DEBUG_T("%s:%d: Finished Recover\n", __FILE__, __LINE__);

    return Status::OK();
}

Status DBImpl::RecoverMapFile(uint64_t map_number, bool *save_manifest,
        VersionEdit *edit, SequenceNumber* max_sequence) {

    Status status;
    std::string fname = MapFileName(dbname_mem_, map_number);
    if (mem_ != NULL) {
        status = WriteLevel0Table(mem_, edit, NULL);
        DEBUG_T("%s:%d: Finished NVM WriteLevel0Table write %s \n",
                __FILE__, __LINE__, fname.c_str());
        mem_->Unref();
        mem_ = NULL;
    }

    MemTable *mem;
    options_.write_buffer_size = nvmbuff_;
    ArenaNVM *arena= new ArenaNVM(options_.write_buffer_size, &fname, true);
    mem = new MemTable(internal_comparator_, *arena, true);
    mem->Ref();
    mem->isNVMMemtable = true;
    *max_sequence = *(uint64_t *)((uint8_t*)arena->getMapStart() + sizeof(size_t));
    mem_ = mem;

#ifdef _ENABLE_DEBUG
    IterateMemAndPrint(mem_);
#endif

    return Status::OK();
}


Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
        bool* save_manifest, VersionEdit* edit,
        SequenceNumber* max_sequence) {
    struct LogReporter : public log::Reader::Reporter {
        Env* env;
        Logger* info_log;
        const char* fname;
        Status* status;  // NULL if options_.paranoid_checks==false
        virtual void Corruption(size_t bytes, const Status& s) {
            Log(info_log, "%s%s: dropping %d bytes; %s",
                    (this->status == NULL ? "(ignoring error) " : ""),
                    fname, static_cast<int>(bytes), s.ToString().c_str());
            if (this->status != NULL && this->status->ok()) *this->status = s;
        }
    };

    mutex_.AssertHeld();

    // Open the log file
    std::string fname = LogFileName(dbname_disk_, log_number);
    SequentialFile* file;
    Status status = env_->NewSequentialFile(fname, &file);
    if (!status.ok()) {
        MaybeIgnoreError(&status);
        return status;
    }

    // Create the log reader.
    LogReporter reporter;
    reporter.env = env_;
    reporter.info_log = options_.info_log;
    reporter.fname = fname.c_str();
    reporter.status = (options_.paranoid_checks ? &status : NULL);

    // We intentionally make log::Reader do checksumming even if
    // paranoid_checks==false so that corruptions cause entire commits
    // to be skipped instead of propagating bad information (like overly
    // large sequence numbers).
    log::Reader reader(file, &reporter, true/*checksum*/,
            0/*initial_offset*/);
    Log(options_.info_log, "Recovering log #%llu",
            (unsigned long long) log_number);

    // Read all the records and add to a memtable
    std::string scratch;
    Slice record;
    WriteBatch batch;
    int compactions = 0;

    while (reader.ReadRecord(&record, &scratch) &&
            status.ok()) {
        if (record.size() < 12) {
            reporter.Corruption(
                    record.size(), Status::Corruption("log record too small"));
            continue;
        }
        WriteBatchInternal::SetContents(&batch, record);

        if (mem_ != NULL && mem_->ApproximateMemoryUsage() >= options_.write_buffer_size) {
            *save_manifest = true;
            compactions++;
            status = WriteLevel0Table(mem_, edit, NULL);
            mem_->Unref();
            mem_ = NULL;
            if (!status.ok()) {
                // Reflect errors immediately so that conditions like full
                // file-systems cause the DB::Open() to fail.
                break;
            }
        }

        if (mem_ == NULL) {
            mem_ = new MemTable(internal_comparator_);
            mem_->isNVMMemtable = false;
            mem_->Ref();
            options_.write_buffer_size = drambuff_;
            logfile_number_ = log_number;
        }

        status = WriteBatchInternal::InsertInto(&batch, mem_);
        MaybeIgnoreError(&status);
        if (!status.ok()) {
            break;
        }
        const SequenceNumber last_seq =
                WriteBatchInternal::Sequence(&batch) +
                WriteBatchInternal::Count(&batch) - 1;
        if (last_seq > *max_sequence) {
            *max_sequence = last_seq;
        }
    }
    delete file;

    // See if we should keep reusing the last log file.
    if (status.ok() && logfile_number_ == log_number) {
        assert(logfile_ == NULL);
        assert(log_ == NULL);
        uint64_t lfile_size;
        if (env_->GetFileSize(fname, &lfile_size).ok() &&
                env_->NewAppendableFile(fname, &logfile_).ok()) {
            log_ = new log::Writer(logfile_, lfile_size);
            logfile_number_ = log_number;
        }
    }
    return status;
}

/* Simple method that iterates any memtable
 * and prints all the entries of the memtable
 */
void DBImpl::IterateMemAndPrint(MemTable* mem) {
    Iterator* iter = mem->NewIterator();

    iter->SeekToFirst();
    for (; iter->Valid(); iter->Next()) {
        Slice key = iter->key();
        Slice val = iter->value();
        const char *key_data = key.data();
        std::cout << "Mem Key: " << key_data << " Size: " << key.size() << "\n";
    }
    return;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
        Version* base) {
    mutex_.AssertHeld();
    const uint64_t start_micros = env_->NowMicros();
    FileMetaData meta;
    meta.number = versions_->NewFileNumber();
    pending_outputs_.insert(meta.number);
    Iterator* iter = mem->NewIterator();
    Log(options_.info_log, "Level-0 table #%llu: started",
            (unsigned long long) meta.number);

    Status s;
    {
        mutex_.Unlock();
        s = BuildTable(dbname_disk_, env_, options_, table_cache_, iter, &meta);
        mutex_.Lock();
    }

    Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
            (unsigned long long) meta.number,
            (unsigned long long) meta.file_size,
            s.ToString().c_str());
    delete iter;
    pending_outputs_.erase(meta.number);


    // Note that if file_size is zero, the file has been deleted and
    // should not be added to the manifest.
    int level = 0;
    if (s.ok() && meta.file_size > 0) {
        const Slice min_user_key = meta.smallest.user_key();
        const Slice max_user_key = meta.largest.user_key();
        if (base != NULL) {
            level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
        }
        edit->AddFile(level, meta.number, meta.file_size,
                meta.smallest, meta.largest);
    }

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros;
    stats.bytes_written = meta.file_size;
    stats_[level].Add(stats);
    return s;
}

void DBImpl::CompactBottomMemTable() {

    mutex_.AssertHeld();
    //NoveLSM: For switching between DRAM and NVM tables
    assert(imm_ != NULL);

    // Save the contents of the memtable as a new Table
    VersionEdit edit;
    Version* base = versions_->current();
    base->Ref();
    Status s;
    int done=0;

    s = WriteLevel0Table(imm_, &edit, base);
    base->Unref();

    // Replace immutable memtable with the generated Table
    if (s.ok()) {
        edit.SetPrevLogNumber(0);
#if defined ENABLE_RECOVERY
        uint64_t max = (logfile_number_ > mapfile_number_) ? logfile_number_ : mapfile_number_;
        edit.SetMapNumber(max);
        edit.SetLogNumber(max);
#else
        edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
#endif
        s = versions_->LogAndApply(&edit, &mutex_);
    }

    if (s.ok() && shutting_down_.Acquire_Load()) {
        s = Status::IOError("Deleting DB during memtable compaction");
    }

    if (s.ok()) {
        // Commit to the new state
        imm_->Unref();
        imm_ = NULL;
        has_imm_.Release_Store(NULL);
        DeleteObsoleteFiles();
    }else {
        RecordBackgroundError(s);
    }
}


void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
    int max_level_with_files = 1;
    {
        MutexLock l(&mutex_);
        Version* base = versions_->current();
        for (int level = 1; level < config::kNumLevels; level++) {
            if (base->OverlapInLevel(level, begin, end)) {
                max_level_with_files = level;
            }
        }
    }
    TEST_CompactMemTable(); // TODO(sanjay): Skip if memtable does not overlap
    for (int level = 0; level < max_level_with_files; level++) {
        TEST_CompactRange(level, begin, end);
    }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,const Slice* end) {
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);

    InternalKey begin_storage, end_storage;

    ManualCompaction manual;
    manual.level = level;
    manual.done = false;
    if (begin == NULL) {
        manual.begin = NULL;
    } else {
        begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
        manual.begin = &begin_storage;
    }
    if (end == NULL) {
        manual.end = NULL;
    } else {
        end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
        manual.end = &end_storage;
    }

    MutexLock l(&mutex_);
    while (!manual.done && !shutting_down_.Acquire_Load() && bg_error_.ok()) {
        if (manual_compaction_ == NULL) {  // Idle
            manual_compaction_ = &manual;
            MaybeScheduleCompaction();
        } else {  // Running either my compaction or another compaction.
            bg_cv_.Wait();
        }
    }
    if (manual_compaction_ == &manual) {
        // Cancel my manual compaction since we aborted early for some reason.
        manual_compaction_ = NULL;
    }
}

Status DBImpl::TEST_CompactMemTable() {
    // NULL batch means just wait for earlier writes to be done
    Status s = Write(WriteOptions(), NULL);
    if (s.ok()) {
        // Wait until the compaction completes
        MutexLock l(&mutex_);
        while (imm_ != NULL && bg_error_.ok()) {
            bg_cv_.Wait();
        }
        if (imm_ != NULL) {
            s = bg_error_;
        }
    }
    return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
    mutex_.AssertHeld();
    if (bg_error_.ok()) {
        bg_error_ = s;
        bg_cv_.SignalAll();
    }
}

void DBImpl::MaybeScheduleCompaction() {
    mutex_.AssertHeld();
    if (bg_compaction_scheduled_) {
        // Already scheduled
    } else if (shutting_down_.Acquire_Load()) {
        // DB is being deleted; no more background compactions
    } else if (!bg_error_.ok()) {
        // Already got an error; no more changes
    }
    else if (imm_ == NULL &&
            manual_compaction_ == NULL &&
            !versions_->NeedsCompaction()) {
        // No work to be done
    }
    else {
        bg_compaction_scheduled_ = true;
        env_->Schedule(&DBImpl::BGWork, this);
    }
}

void DBImpl::ScheduleCompactionNow() {
    mutex_.AssertHeld();
    if (bg_compaction_scheduled_) {
        // Already scheduled
    }else {
        bg_compaction_scheduled_ = true;
        env_->Schedule(&DBImpl::BGWork, this);
    }
}

void DBImpl::BGWork(void* db) {
    reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}

void DBImpl::skiplistBackgroundSync(void *db) {
    MemTable* tmp_mem = reinterpret_cast<DBImpl*>(db)->mem_;
    int pending, index;
    for(int i=0; i<tmp_mem->arena_.sub_mem_count; i++) {
        if(tmp_mem->arena_.sub_mem_bset[i] && !tmp_mem->arena_.in_trans_bset[i].load() && !tmp_mem->arena_.in_trans_bset[i].exchange(1)) {
            pending = tmp_mem->sub_mem_pending_node[i].size();
            index = tmp_mem->sub_mem_pending_node_index[i];
            if(index < pending) {
                char *buf;
                for(int j=index; j<pending; j++){
                    buf = tmp_mem->sub_mem_pending_node[i][j];
                    tmp_mem->sub_mem_skiplist[i].Insert(buf);
                }
                tmp_mem->sub_mem_pending_node_index[i] = pending;
            }
            tmp_mem->arena_.in_trans_bset[i].store(0);
        }
    }
    reinterpret_cast<DBImpl*>(db)->inSkiplistBgSync.store(0);
}



void DBImpl::subImmToImm(void *work) {
    work_struct *p = (work_struct*)work;
    void *db = (void*)p->db;
    int ImmNum = p->index;
    free(p);
    if(ImmNum > reinterpret_cast<DBImpl*>(db)->subImm_thread){
	    return ;
    }
    MemTable* tmp_mem = reinterpret_cast<DBImpl*>(db)->mem_;
    int sub_imm_index = -1;
    int n = tmp_mem->arena_.sub_mem_count / reinterpret_cast<DBImpl*>(db)->subImm_thread;
    int begin = n*ImmNum;
    int end = begin + n;
    if(!reinterpret_cast<DBImpl*>(db)->subImm_partition) {
        begin = 0;
        end = tmp_mem->arena_.sub_mem_count;
    }

retry:
    for(int i=begin; i<end; i++)
        if(tmp_mem->arena_.sub_immem_bset[i].load() 
        && tmp_mem->arena_.sub_immem_bset[i].exchange(0)) {
            sub_imm_index = i;
            break;
        }
            
    if(sub_imm_index == -1) {
        if(reinterpret_cast<DBImpl*>(db)->subImmKill) {
            subImmCount--;
            return;
        }
        goto retry;
    }
    tmp_mem->arena_.sub_immem_count--;
    while(1) {
        if(!tmp_mem->arena_.in_trans_bset[sub_imm_index].load() && !tmp_mem->arena_.in_trans_bset[sub_imm_index].exchange(1))
            break;
    }

    MemTable *imm = reinterpret_cast<DBImpl*>(db)->CreateNVMtable(false);
    ArenaNVM *imm_arena = (ArenaNVM*)&imm->arena_;
    imm_arena->isDataLock = 0;
    imm_arena->AllocateFallbackNVM(SUB_MEM_SIZE);

    int pending = tmp_mem->sub_mem_pending_node[sub_imm_index].size();
    int index = tmp_mem->sub_mem_pending_node_index[sub_imm_index];
    if(index < pending) {
        char *buf;
        for(int j=index; j<pending; j++){
            buf = tmp_mem->sub_mem_pending_node[sub_imm_index][j];
            tmp_mem->sub_mem_skiplist[sub_imm_index].Insert(buf);
        }
        tmp_mem->sub_mem_pending_node_index[sub_imm_index] = pending;
    }

    memcpy(imm->arena_.map_start_, tmp_mem->arena_.map_start_ + SUB_MEM_SIZE * sub_imm_index, SUB_MEM_SIZE);
    for(int i=0; i < tmp_mem->table_.kMaxHeight; i++) {
        imm->table_.head_->SetNext(i, tmp_mem->sub_mem_skiplist[sub_imm_index].head_->Next(i));
        tmp_mem->sub_mem_skiplist[sub_imm_index].head_->SetNext(i, NULL);
    }
    MemTable::Table::Iterator iter(&imm->table_);
    char* new_off;
    char *off;
    off = (char*)imm->arena_.map_start_ - (char*)(tmp_mem->arena_.map_start_ + SUB_MEM_SIZE * sub_imm_index);
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        new_off = (intptr_t)iter.key_offset() + off;
        iter.set_key_offset(new_off);
    }

    tmp_mem->sub_mem_pending_node[sub_imm_index].clear();
    tmp_mem->sub_mem_pending_node_index[sub_imm_index] = 0;

    imm->arena_.blocks_ = tmp_mem->arena_.skiplist_blocks[sub_imm_index];
    tmp_mem->arena_.skiplist_blocks[sub_imm_index].clear();

    tmp_mem->arena_.sub_immem_bset[sub_imm_index].store(false);
    tmp_mem->arena_.sub_mem_bset[sub_imm_index].store(false);

    while(tmp_mem->isQueBusy.load());
    tmp_mem->subImmQue.push_front(imm);
    tmp_mem->arena_.in_trans_bset[sub_imm_index].store(0);

    sub_imm_index = -1;
    goto retry;
}

void DBImpl::compactImm(void* db) {
    if(!reinterpret_cast<DBImpl*>(db)->inCompactImm.load() 
    && !reinterpret_cast<DBImpl*>(db)->inCompactImm.exchange(1)){

    }
    else{
        return;
    }

    MemTable* tmp_mem = reinterpret_cast<DBImpl*>(db)->mem_;
    MemTable* sub_imm;
    std::deque<MemTable*> tmp_subImmQue;
loop:
    if(!tmp_mem->isQueBusy.load() && !tmp_mem->isQueBusy.exchange(1)){
        std::swap(tmp_subImmQue, tmp_mem->subImmQue);
    }
    else{
        return;
    }
    tmp_mem->isQueBusy.store(0);

    for(int i=0; i<tmp_subImmQue.size(); i++) {
        sub_imm = tmp_subImmQue[i];
        MemTable::Table::Iterator iter(&(sub_imm->table_));
        iter.SeekToFirst();
        void *p;
        while(iter.Valid()){
            p = (void*)iter.node_;
            iter.Next();
            tmp_mem->table_.InsertNode(p);
        }
        reinterpret_cast<DBImpl*>(db)->compactImmQue.push_back(sub_imm);
    }
    tmp_subImmQue.clear();
    if(tmp_mem->subImmQue.size() > reinterpret_cast<DBImpl*>(db)->compactImm_threshold){
		goto loop;
	}
    reinterpret_cast<DBImpl*>(db)->inCompactImm.store(0);
}


void DBImpl::BackgroundCall() {
    MutexLock l(&mutex_);
    assert(bg_compaction_scheduled_);
    if (shutting_down_.Acquire_Load()) {
        // No more background work when shutting down.
    } else if (!bg_error_.ok()) {
        // No more background work after a background error.
    } else {
        BackgroundCompaction();
    }

    bg_compaction_scheduled_ = false;

    // Previous compaction may have produced too many files in a level,
    // so reschedule another compaction if needed.
    MaybeScheduleCompaction();
    bg_cv_.SignalAll();
}

void DBImpl::BackgroundCompaction() {

    mutex_.AssertHeld();

    if (versions_->NumLevelFiles(0) < config::kL0_SlowdownWritesTrigger && imm_ != NULL) {
        CompactBottomMemTable();
        return;
    }

    Compaction* c;
    bool is_manual = (manual_compaction_ != NULL);
    InternalKey manual_end;
    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        c = versions_->CompactRange(m->level, m->begin, m->end);
        m->done = (c == NULL);
        if (c != NULL) {
            manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
        }
        Log(options_.info_log,
                "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
                m->level,
                (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
                (m->end ? m->end->DebugString().c_str() : "(end)"),
                (m->done ? "(end)" : manual_end.DebugString().c_str()));
    } else {
        c = versions_->PickCompaction();
    }

    Status status;
    if (c == NULL) {
        // Nothing to do
    } else if (!is_manual && c->IsTrivialMove()) {
        // Move file to next level
        assert(c->num_input_files(0) == 1);
        FileMetaData* f = c->input(0, 0);
        c->edit()->DeleteFile(c->level(), f->number);
        c->edit()->AddFile(c->level() + 1, f->number, f->file_size,
                f->smallest, f->largest);
        status = versions_->LogAndApply(c->edit(), &mutex_);
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        VersionSet::LevelSummaryStorage tmp;
        Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
                static_cast<unsigned long long>(f->number),
                c->level() + 1,
                static_cast<unsigned long long>(f->file_size),
                status.ToString().c_str(),
                versions_->LevelSummary(&tmp));
    } else {
        CompactionState* compact = new CompactionState(c);
        status = DoCompactionWork(compact);
        if (!status.ok()) {
            RecordBackgroundError(status);
        }
        CleanupCompaction(compact);
        c->ReleaseInputs();
        DeleteObsoleteFiles();
    }
    delete c;

    if (status.ok()) {
        // Done
    } else if (shutting_down_.Acquire_Load()) {
        // Ignore compaction errors found during shutting down
    } else {
        Log(options_.info_log,
                "Compaction error: %s", status.ToString().c_str());
    }

    if (is_manual) {
        ManualCompaction* m = manual_compaction_;
        if (!status.ok()) {
            m->done = true;
        }
        if (!m->done) {
            // We only compacted part of the requested range.  Update *m
            // to the range that is left to be compacted.
            m->tmp_storage = manual_end;
            m->begin = &m->tmp_storage;
        }
        manual_compaction_ = NULL;
    }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
    mutex_.AssertHeld();
    if (compact->builder != NULL) {
        // May happen if we get a shutdown call in the middle of compaction
        compact->builder->Abandon();
        delete compact->builder;
    } else {
        assert(compact->outfile == NULL);
    }
    delete compact->outfile;
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output& out = compact->outputs[i];
        pending_outputs_.erase(out.number);
    }
    delete compact;
}

Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
    assert(compact != NULL);
    assert(compact->builder == NULL);
    uint64_t file_number;
    {
        mutex_.Lock();
        file_number = versions_->NewFileNumber();
        pending_outputs_.insert(file_number);
        CompactionState::Output out;
        out.number = file_number;
        out.smallest.Clear();
        out.largest.Clear();
        compact->outputs.push_back(out);
        mutex_.Unlock();
    }

    std::string fname = TableFileName(dbname_disk_, file_number);
    Status s = env_->NewWritableFile(fname, &compact->outfile);
    if (s.ok()) {
        compact->builder = new TableBuilder(options_, compact->outfile);
    }
    return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
        Iterator* input) {
    assert(compact != NULL);
    assert(compact->outfile != NULL);
    assert(compact->builder != NULL);

    const uint64_t output_number = compact->current_output()->number;
    assert(output_number != 0);

    // Check for iterator errors
    Status s = input->status();
    const uint64_t current_entries = compact->builder->NumEntries();
    if (s.ok()) {
        s = compact->builder->Finish();
    } else {
        compact->builder->Abandon();
    }
    const uint64_t current_bytes = compact->builder->FileSize();
    compact->current_output()->file_size = current_bytes;
    compact->total_bytes += current_bytes;
    delete compact->builder;
    compact->builder = NULL;

    // Finish and check for file errors
    if (s.ok()) {
        s = compact->outfile->Sync();
    }
    if (s.ok()) {
        s = compact->outfile->Close();
    }
    delete compact->outfile;
    compact->outfile = NULL;

    if (s.ok() && current_entries > 0) {
        // Verify that the table is usable
        Iterator* iter = table_cache_->NewIterator(ReadOptions(),
                output_number,
                current_bytes);
        s = iter->status();
        delete iter;
        if (s.ok()) {
            Log(options_.info_log,
                    "Generated table #%llu@%d: %lld keys, %lld bytes",
                    (unsigned long long) output_number,
                    compact->compaction->level(),
                    (unsigned long long) current_entries,
                    (unsigned long long) current_bytes);
        }
    }
    return s;
}

Status DBImpl::InstallCompactionResults(CompactionState* compact) {
    mutex_.AssertHeld();
    Log(options_.info_log,  "Compacted %d@%d + %d@%d files => %lld bytes",
            compact->compaction->num_input_files(0),
            compact->compaction->level(),
            compact->compaction->num_input_files(1),
            compact->compaction->level() + 1,
            static_cast<long long>(compact->total_bytes));

    // Add compaction outputs
    compact->compaction->AddInputDeletions(compact->compaction->edit());
    const int level = compact->compaction->level();
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        const CompactionState::Output& out = compact->outputs[i];
        compact->compaction->edit()->AddFile(
                level + 1,
                out.number, out.file_size, out.smallest, out.largest);
    }
    return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}


Status DBImpl::DoCompactionWork(CompactionState* compact) {
    const uint64_t start_micros = env_->NowMicros();
    int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

    Log(options_.info_log,  "Compacting %d@%d + %d@%d files",
            compact->compaction->num_input_files(0),
            compact->compaction->level(),
            compact->compaction->num_input_files(1),
            compact->compaction->level() + 1);

    assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
    assert(compact->builder == NULL);
    assert(compact->outfile == NULL);
    if (snapshots_.empty()) {
        compact->smallest_snapshot = versions_->LastSequence();
    } else {
        compact->smallest_snapshot = snapshots_.oldest()->number_;
    }

    // Release mutex while we're actually doing the compaction work
    mutex_.Unlock();

    Iterator* input = versions_->MakeInputIterator(compact->compaction);
    input->SeekToFirst();
    Status status;
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

    for (; input->Valid() && !shutting_down_.Acquire_Load(); ) {
        // Prioritize immutable compaction work
        if (has_imm_.NoBarrier_Load() != NULL) {
            const uint64_t imm_start = env_->NowMicros();
            mutex_.Lock();
            if (imm_ != NULL) {
                CompactBottomMemTable();
                bg_cv_.SignalAll();  // Wakeup MakeRoomForWrite() if necessary
            }
            mutex_.Unlock();
            imm_micros += (env_->NowMicros() - imm_start);
        }
        Slice key = input->key();
        if (compact->compaction->ShouldStopBefore(key) &&
                compact->builder != NULL) {
            status = FinishCompactionOutputFile(compact, input);
            if (!status.ok()) {
                break;
            }
        }

        // Handle key/value, add to state, etc.
        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            // Do not hide error keys
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key ||
                    user_comparator()->Compare(ikey.user_key,
                            Slice(current_user_key)) != 0) {
                // First occurrence of this user key
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
            }

            if (last_sequence_for_key <= compact->smallest_snapshot) {
                // Hidden by an newer entry for same user key
                drop = true;    // (A)
            } else if (ikey.type == kTypeDeletion &&
                    ikey.sequence <= compact->smallest_snapshot &&
                    compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
                // For this user key:
                // (1) there is no data in higher levels
                // (2) data in lower levels will have larger sequence numbers
                // (3) data in layers that are being compacted here and have
                //     smaller sequence numbers will be dropped in the next
                //     few iterations of this loop (by rule (A) above).
                // Therefore this deletion marker is obsolete and can be dropped.
                drop = true;
            }

            last_sequence_for_key = ikey.sequence;
        }

        if (!drop) {
            // Open output file if necessary
            if (compact->builder == NULL) {
                status = OpenCompactionOutputFile(compact);
                if (!status.ok()) {
                    break;
                }
            }
            if (compact->builder->NumEntries() == 0) {
                compact->current_output()->smallest.DecodeFrom(key);
            }
            compact->current_output()->largest.DecodeFrom(key);
            compact->builder->Add(key, input->value());

            // Close output file if it is big enough
            if (compact->builder->FileSize() >=
                    compact->compaction->MaxOutputFileSize()) {
                status = FinishCompactionOutputFile(compact, input);
                if (!status.ok()) {
                    break;
                }
            }
        }
        input->Next();
    }

    if (status.ok() && shutting_down_.Acquire_Load()) {
        status = Status::IOError("Deleting DB during compaction");
    }
    if (status.ok() && compact->builder != NULL) {
        status = FinishCompactionOutputFile(compact, input);
    }
    if (status.ok()) {
        status = input->status();
    }
    delete input;
    input = NULL;

    CompactionStats stats;
    stats.micros = env_->NowMicros() - start_micros - imm_micros;
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
            stats.bytes_read += compact->compaction->input(which, i)->file_size;
        }
    }
    for (size_t i = 0; i < compact->outputs.size(); i++) {
        stats.bytes_written += compact->outputs[i].file_size;
    }

    mutex_.Lock();
    stats_[compact->compaction->level() + 1].Add(stats);

    if (status.ok()) {
        status = InstallCompactionResults(compact);
    }
    if (!status.ok()) {
        RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log,
            "compacted to: %s", versions_->LevelSummary(&tmp));
    return status;
}

namespace {
struct IterState {
    port::Mutex* mu;
    Version* version;
    MemTable* mem;
    MemTable* imm;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
    IterState* state = reinterpret_cast<IterState*>(arg1);
    state->mu->Lock();
    state->mem->Unref();
    if (state->imm != NULL) state->imm->Unref();
    state->version->Unref();
    state->mu->Unlock();
    delete state;
}
}  // namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
        SequenceNumber* latest_snapshot,
        uint32_t* seed) {
    IterState* cleanup = new IterState;
    mutex_.Lock();
    *latest_snapshot = versions_->LastSequence();

    // Collect together all needed child iterators
    std::vector<Iterator*> list;
    list.push_back(mem_->NewIterator());
    mem_->Ref();

    if(!inSkiplistBgSync.load() && !inSkiplistBgSync.exchange(1)) {
        skiplistBackgroundSync((void*)this);
    }
    else {
        while(!inSkiplistBgSync.load());
    }

    if(mem_->subImmQue.size() && !inCompactImm.load()){
	    compactImm((void*)this);
    }

    for(int i=0; i<mem_->arena_.sub_mem_count; i++) {
        if(mem_->arena_.sub_mem_bset[i].load() || mem_->arena_.sub_immem_bset[i].load())
	        list.push_back(mem_->NewSubMemIterator(i));
    }
    versions_->current()->AddIterators(options, &list);
    Iterator* internal_iter =
            NewMergingIterator(&internal_comparator_, &list[0], list.size());
    versions_->current()->Ref();

    cleanup->mu = &mutex_;
    cleanup->mem = mem_;
    cleanup->imm = imm_;
    cleanup->version = versions_->current();
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, NULL);

    *seed = ++seed_;
    mutex_.Unlock();
    return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
    SequenceNumber ignored;
    uint32_t ignored_seed;
    return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
    MutexLock l(&mutex_);
    return versions_->MaxNextLevelOverlappingBytes();
}

void* DBImpl::read_thread(void *arg) {

    read_struct *str = (read_struct *)(arg);
    bool ret = false;
    int val = str->val;
    std::string *value = str->value;
    Status *s = str->s;
    LookupKey *lkey =  str->lkey;
    Version* current = str->current;

    switch (val) {

    case MEMTBL_THRD:
        if (!kCheckCond && (ret = g_mem->Get(*lkey, value, s))) {
            current->SetTerminate();
            kCheckCond = 1;
            incr_mem_hits();
            str->done = true;
        } else if (!kCheckCond && g_imm != NULL &&
                (ret = g_imm->Get(*lkey, value, s))) {
            current->SetTerminate();
            kCheckCond = 1;
            incr_imm_hits();
            str->done = true;
        }
        break;
    case SSTBL_THRD:
        //Version* current = str->current;
        Version::GetStats *stats;
        stats = (Version::GetStats *)str->stats;
        if(current){
            Status s = current->Get((const leveldb::ReadOptions&)g_options,
                    *lkey, value, stats);
            if(s.ok()) {
                kCheckCond = 1;
                ret = true;
                incr_sstable_hits();
                str->done = true;
            }
            else {
                ret = false;
                str->done = false;
            }
            //*(str->have_stat_update)  = true;
        }
        break;
    default:
        return NULL;
    }
    return NULL;
}

/*Order should be preserved for
 * incrementing flags
 */
void DBImpl::IncrementHitFlag(){

    if(mem_found == true) {
        mem_hits++;
        goto incr_done;
    }
    else if(sstable_found == true) {
        sstable_hits++;
        goto incr_done;
    }

    incr_done:
    return;
}

bool DBImpl::CheckSearchCondition(MemTable* mem){
    if (mem == NULL)
        return false;
    if (mem->isNVMMemtable == true){
        return true;
    }
    if (!mem->GetNumKeys())
        return false;

    return true;
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  Status s;

  if(!inSkiplistBgSync.load() && !inSkiplistBgSync.exchange(1)) {
    skiplistBackgroundSync((void*)this);
  }
  else {
    while(!inSkiplistBgSync.load());
  }

  if(mem_->subImmQue.size() && !inCompactImm.load()){
	compactImm((void*)this);
  }

  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != NULL) {
    snapshot = reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  g_mem = mem_;
  mem->Ref();

  Version::GetStats stats;

  {
    mutex_.Unlock();
    LookupKey lkey(key, snapshot);
    if(mem->Get_submem(lkey, value, &s)) {}
    else {
        mem_->Get(lkey, value, &s);
    }
end:
    mutex_.Lock();
  }

  mem->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    SequenceNumber latest_snapshot;
    uint32_t seed;
    Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
    return NewDBIterator(
            this, user_comparator(), iter,
            (options.snapshot != NULL
                    ? reinterpret_cast<const SnapshotImpl*>(options.snapshot)->number_
                            : latest_snapshot),
                              seed);
}

void DBImpl::RecordReadSample(Slice key) {
    MutexLock l(&mutex_);
    if (versions_->current()->RecordReadSample(key)) {
        MaybeScheduleCompaction();
    }
}

const Snapshot* DBImpl::GetSnapshot() {
    MutexLock l(&mutex_);
    return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
    MutexLock l(&mutex_);
    snapshots_.Delete(reinterpret_cast<const SnapshotImpl*>(s));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
    return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* my_batch) {
    Writer w(&mutex_);
    w.batch = my_batch;
    w.sync = options.sync;
    w.done = false;

    Status status;
    status = MakeRoomForWrite(my_batch == NULL);

    if (status.ok() && my_batch != NULL) { 
        WriteBatch* updates = my_batch;

        {
            status = Status::OK();
            if (status.ok()) {
                status = WriteBatchInternal::InsertInto(updates, mem_);
            }
        }
        versions_->SetLastSequence(WriteBatchInternal::Count(updates));
    }
    assert(mem_->GetNumKeys());
    return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-NULL batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
    assert(!writers_.empty());
    Writer* first = writers_.front();
    WriteBatch* result = first->batch;
    assert(result != NULL);

    size_t size = WriteBatchInternal::ByteSize(first->batch);

    // Allow the group to grow up to a maximum size, but if the
    // original write is small, limit the growth so we do not slow
    // down the small write too much.
    size_t max_size = 1 << 20;
    if (size <= (128<<10)) {
        max_size = size + (128<<10);
    }

    *last_writer = first;
    std::deque<Writer*>::iterator iter = writers_.begin();
    ++iter;  // Advance past "first"
    for (; iter != writers_.end(); ++iter) {
        Writer* w = *iter;
        if (w->sync && !first->sync) {
            // Do not include a sync write into a batch handled by a non-sync write.
            break;
        }
        if (w->batch != NULL) {
            size += WriteBatchInternal::ByteSize(w->batch);
            if (size > max_size) {
                // Do not make batch too big
                break;
            }

            // Append to *result
            if (result == first->batch) {
                // Switch to temporary batch instead of disturbing caller's batch
                result = tmp_batch_;
                assert(WriteBatchInternal::Count(result) == 0);
                WriteBatchInternal::Append(result, first->batch);
            }
            WriteBatchInternal::Append(result, w->batch);
        }
        *last_writer = w;
    }
    return result;
}

/* Creates DRAM memtable
 * Also creates a log file for DRAM memtable
 */
MemTable* DBImpl::CreateMemTable(void) {
    Status s;
    MemTable* mem;
#if defined (ENABLE_RECOVERY)
    uint64_t new_log_number = versions_->NewFileNumber();
    WritableFile* lfile = NULL;
    s = env_->NewWritableFile(LogFileName(dbname_disk_, new_log_number), &lfile);
    delete log_;
    delete logfile_;
    logfile_number_ = new_log_number;
    logfile_ = lfile;
    log_ = new log::Writer(lfile);
#endif
    mem = new MemTable(internal_comparator_);
    mem->isNVMMemtable = false;
    assert(mem);
    return mem;
}

/* Creates NVM memtable
 * Also allocates corresponding NVM arena
 * Skip list node allocations are from NVM arena
 */
MemTable* DBImpl::CreateNVMtable(bool assign_map){

    MemTable* mem;
#ifdef ENABLE_RECOVERY
    uint64_t new_map_number = versions_->NewFileNumber();
    size_t size = 0;
    std::string filename = MapFileName(dbname_mem_, new_map_number);
    size = nvmbuff_;
    if (!assign_map)
        mapfile_number_ = new_map_number;
    ArenaNVM *arena= new ArenaNVM(size, &filename, false);
#else
    ArenaNVM *arena= new ArenaNVM();
#endif
    mem = new MemTable(internal_comparator_, *arena, false);
    mem->isNVMMemtable = true;
    assert(mem);
    return mem;
}

/* Alternates between DRAM and NVM memtable
 * and sets their appropriate size
 *
 */
int DBImpl::SwapMemtables() {

    static uint64_t count = 0;

    //When enabled NoveLSM alternates between mem and mem2_ in NVM tables
    if (use_multiple_levels) {
        if (!mem_->isNVMMemtable) {
            options_.write_buffer_size = nvmbuff_;
            mem_ =  CreateNVMtable(false);
            mem_->isNVMMemtable = true;
        } else {
            options_.write_buffer_size = drambuff_;
            mem_ = CreateMemTable();
        }
    } else {
        assert(0);
    }

    return 0;
}

/* Method responsible for compaction and
 * making room for DRAM memtable
 */
Status DBImpl::MakeRoomForWrite(bool force) {
    mutex_.AssertHeld();
    assert(!writers_.empty());
    Status s;

    while (true) {
        int tmp_imm_count = 0;
        int tmp_mem_count = 0;
        for(int i=0; i<mem_->arena_.sub_mem_count; i++) {
            if(mem_->arena_.sub_immem_bset[i].load())
                tmp_imm_count++;
            if(mem_->arena_.sub_mem_bset[i].load())
                tmp_mem_count++;
        }
        if (!bg_error_.ok()) {
            // Yield previous error
            s = bg_error_;
            break;
        } else if (tmp_imm_count && !subImmCount.exchange(1)) {
            work_struct *job;
            for(int i=0; i<subImm_thread; i++) {
                job = (work_struct*)malloc(sizeof(work_struct));
                job->db = this;
                job->index = i;
                env_->Schedule(&DBImpl::subImmToImm, (void*)job);
                job = NULL;
            }
            break;
        } else if(compactImm_threshold>0 && mem_->subImmQue.size()>compactImm_threshold && !inCompactImm.load()) {
                env_->Schedule(&DBImpl::compactImm, (void*)this);
        } else if (tmp_mem_count < mem_->arena_.sub_mem_count)
                break;
    }

    if(skiplistSync_threshold>0 && mem_->GetNumKeys()>1 && (mem_->GetNumKeys() % skiplistSync_threshold == 1) 
    && !inSkiplistBgSync.load() && !inSkiplistBgSync.exchange(1)) {
        env_->Schedule(&DBImpl::skiplistBackgroundSync, (void*)this);
    }

    return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
    value->clear();

    MutexLock l(&mutex_);
    Slice in = property;
    Slice prefix("leveldb.");
    if (!in.starts_with(prefix)) return false;
    in.remove_prefix(prefix.size());

    if (in.starts_with("num-files-at-level")) {
        in.remove_prefix(strlen("num-files-at-level"));
        uint64_t level;
        bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
        if (!ok || level >= config::kNumLevels) {
            return false;
        } else {
            char buf[100];
            snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
            *value = buf;
            return true;
        }
    } else if (in == "stats") {
        char buf[200];
        snprintf(buf, sizeof(buf),
                "                               Compactions\n"
                "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                "--------------------------------------------------\n"
        );
        value->append(buf);
        for (int level = 0; level < config::kNumLevels; level++) {
            int files = versions_->NumLevelFiles(level);
            if (stats_[level].micros > 0 || files > 0) {
                snprintf(
                        buf, sizeof(buf),
                        "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                        level,
                        files,
                        versions_->NumLevelBytes(level) / 1048576.0,
                        stats_[level].micros / 1e6,
                        stats_[level].bytes_read / 1048576.0,
                        stats_[level].bytes_written / 1048576.0);
                value->append(buf);
            }
        }
        return true;
    } else if (in == "sstables") {
        *value = versions_->current()->DebugString();
        return true;
    } else if (in == "approximate-memory-usage") {
        size_t total_usage = options_.block_cache->TotalCharge();
        if (mem_) {
            total_usage += mem_->ApproximateMemoryUsage();
        }
        if (imm_) {
            total_usage += imm_->ApproximateMemoryUsage();
        }
        char buf[50];
        snprintf(buf, sizeof(buf), "%llu",
                static_cast<unsigned long long>(total_usage));
        value->append(buf);
        return true;
    }

    return false;
}

void DBImpl::GetApproximateSizes(
        const Range* range, int n,
        uint64_t* sizes) {
    // TODO(opt): better implementation
    Version* v;
    {
        MutexLock l(&mutex_);
        versions_->current()->Ref();
        v = versions_->current();
    }

    for (int i = 0; i < n; i++) {
        // Convert user_key into a corresponding internal key.
        InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
        InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
        uint64_t start = versions_->ApproximateOffsetOf(v, k1);
        uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
        sizes[i] = (limit >= start ? limit - start : 0);
    }

    {
        MutexLock l(&mutex_);
        v->Unref();
    }
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(opt, &batch);
}

static int
init_pqos(void)
{
    const struct pqos_cpuinfo *p_cpu = NULL;
    const struct pqos_cap *p_cap = NULL;
    struct pqos_config cfg;
    int ret;

    memset(&cfg, 0, sizeof(cfg));
    cfg.fd_log = STDOUT_FILENO;
    cfg.verbose = 0;
    ret = pqos_init(&cfg);
    if (ret != PQOS_RETVAL_OK) {
        printf("Error initializing PQoS library!\n");
        return -1;
    }

    ret = pqos_cap_get(&p_cap, &p_cpu);
    if (ret != PQOS_RETVAL_OK) {
        pqos_fini();
        printf("Error retrieving PQoS capabilities!\n");
        return -1;
    }

    ret = pqos_alloc_reset(PQOS_REQUIRE_CDP_ANY, PQOS_REQUIRE_CDP_ANY,
                            PQOS_MBA_ANY);
    if (ret != PQOS_RETVAL_OK) {
        pqos_fini();
        printf("Error resetting CAT!\n");
        return -1;
    }

    return 0;
}

static int
close_pqos(void)
{
    int ret_val = 0;

    if (pqos_fini() != PQOS_RETVAL_OK) {
        printf("Error shutting down PQoS library!\n");
        ret_val = -1;
    }

    return ret_val;
}

DB::~DB() {
    close_pqos();
 }

Status DB::Open(const Options& options, const std::string& dbname_disk,
        const std::string& dbname_mem, DB** dbptr) {
    *dbptr = NULL;
    DBImpl* impl = new DBImpl(options, dbname_disk, dbname_mem);
    impl->mutex_.Lock();
    VersionEdit edit;

    size_t subMemSize = SUB_MEM_SIZE;
    subImmCount.store(0);

    // Recover handles create_if_missing, error_if_exists
    bool save_manifest = false;
    Status s = impl->Recover(&edit, &save_manifest);

#ifdef ENABLE_RECOVERY
    if (s.ok() && impl->mem_ == NULL) {

        DEBUG_T("%s:%d: Finished Recovery \n",__FILE__, __LINE__);
#else
        if (s.ok() && (impl->mem_ == NULL)) {
#endif
            // Create new log and a corresponding memtable.
            uint64_t new_log_number = impl->versions_->NewFileNumber();
            WritableFile* lfile;
            //Save log in disk for now
            s = options.env->NewWritableFile(LogFileName(dbname_disk, new_log_number),
                    &lfile);
            if (s.ok()) {
                edit.SetLogNumber(new_log_number);
                impl->logfile_ = lfile;
                impl->log_ = new log::Writer(lfile);
                if (impl->mem_ == NULL) {
                    if (init_pqos() != 0) {
                        (void)close_pqos();
                    }
#if defined(ENABLE_RECOVERY)
                    uint64_t new_map_number = impl->versions_->NewFileNumber();
                    size_t size = 0;
                    std::string filename = MapFileName(impl->dbname_mem_, new_map_number);
                    size = impl->nvmbuff_;
                    impl->mapfile_number_ = new_map_number;
                    ArenaNVM *arena= new ArenaNVM(size, &filename, false);
                    if(impl->isFirstArena) {
                        arena->isDataLock = impl->isFirstArena;
                        arena->dlock_way = options.dlock_way;
                        arena->dlock_size = options.dlock_size;
                        impl->isFirstArena = 0;
                        arena->Allocate(1);
                        arena->reclaim_sub_mem(-1);
                    }
                    else
                        arena->isDataLock = impl->isFirstArena;
#else
                    ArenaNVM *arena= new ArenaNVM();
#endif
                    impl->mem_ = new MemTable(impl->internal_comparator_, *arena, false);
                    impl->mem_->isNVMMemtable = true;

#if defined(ENABLE_RECOVERY)
                    impl->logfile_number_ = new_log_number;
#else
                    impl->mem_->logfile_number = impl->logfile_number_ = new_log_number;
#endif
                    impl->mem_->Ref();
                }
            }
        }

        if (s.ok() && save_manifest) {
            edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
#ifdef ENABLE_RECOVERY
            uint64_t max = (impl->logfile_number_ > impl->mapfile_number_) ? impl->logfile_number_ : impl->mapfile_number_;
            edit.SetLogNumber(max);
            edit.SetMapNumber(max);
#else
            edit.SetLogNumber(impl->logfile_number_);
#endif
            s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
        }
        if (s.ok()) {
            impl->DeleteObsoleteFiles();
            impl->MaybeScheduleCompaction();
        }
        impl->mutex_.Unlock();
        if (s.ok()) {
            assert(impl->mem_ != NULL);
            *dbptr = impl;
        } else {
            delete impl;
        }
        return s;
    }

    Snapshot::~Snapshot() {
    }

    Status DestroyDB(const std::string& dbname_disk, const std::string& dbname_mem, const Options& options) {
        Env* env = options.env;
        std::vector<std::string> filenames;
        std::vector<std::string> filenames_mem;
        // Ignore error in case directory does not exist
        env->GetChildren(dbname_disk, &filenames);
        if (filenames.empty()) {
            env->GetChildren(dbname_mem, &filenames_mem);
            if (filenames_mem.empty()) {
                return Status::OK();
            }
        }
        filenames.insert(filenames.end(), filenames_mem.begin(), filenames_mem.end());

        FileLock* lock;
        const std::string lockname = LockFileName(dbname_disk);
        Status result = env->LockFile(lockname, &lock);
        if (result.ok()) {
            uint64_t number;
            FileType type;
            for (size_t i = 0; i < filenames.size(); i++) {
                if (ParseFileName(filenames[i], &number, &type) &&
                        type != kDBLockFile) {  // Lock file will be deleted at end
                    Status del;
                    if (find(filenames_mem.begin(), filenames_mem.end(), filenames[i]) != filenames_mem.end())
                        del = env->DeleteFile(dbname_mem + "/" + filenames[i]);
                    else
                        del = env->DeleteFile(dbname_disk + "/" + filenames[i]);
                    if (result.ok() && !del.ok()) {
                        result = del;
                    }
                }
            }
            env->UnlockFile(lock);  // Ignore error since state is already gone
            env->DeleteFile(lockname);
            env->DeleteDir(dbname_disk);  // Ignore error in case dir contains other files
            //env->DeleteDir(dbname_mem);  // Dont delete ramdisk folder for now
        }
        return result;
    }

}  // namespace leveldb
