// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <cstdlib>
#include "util/arena.h"
#include <assert.h>
#include "hoard/heaplayers/wrappers/gnuwrapper.h"
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>
#include <atomic>
#define SKIPLIST_ALLOC_SIZE 2097152 

#include <pqos.h>
#define MAX_L3CAT_NUM 16
#define DIM(x)        (sizeof(x) / sizeof(x[0]))
static struct {
        unsigned id;
        struct pqos_l3ca *cos_tab;
} m_l3cat_cos[MAX_L3CAT_NUM];
struct pqos_cpuinfo *g_p_cpu = NULL;
struct pqos_cap *g_p_cap = NULL;
static unsigned m_num_clos = 0;

static const long kBlockSize = 4096;
static int mmap_count = 0;

namespace leveldb {
Arena::Arena()
: memory_usage_(0)
{
    nvmarena_ = false;
    alloc_ptr_ = NULL;  // First allocation will allocate a block
    alloc_bytes_remaining_ = 0;
    map_start_ = map_end_ = 0;
    is_largemap_set_ = 0;
    nvmarena_ = false;
    fd = -1;
    kSize = kBlockSize;
}


Arena::~Arena() {
#ifdef ENABLE_RECOVERY
    for (size_t i = 0; i < blocks_.size(); i++) {
        if(this->nvmarena_ == true) {
            assert(i <= 0);
            assert(kSize != kBlockSize);
            munmap(blocks_[i], kSize);
        }
        else
            delete[] blocks_[i];
    }
    if (fd != -1)
        close(fd);
#else
    for (size_t i = 0; i < blocks_.size(); i++) {
        delete[] blocks_[i];
    }
#endif
}

void* Arena:: operator new(size_t size)
{
    return malloc(size);
}

void* Arena::operator new[](size_t size) {
    return malloc(size);
}

void Arena::operator delete(void* ptr)
{
    free(ptr);
}

void* Arena::CalculateOffset(void* ptr) {
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) - reinterpret_cast<intptr_t>(map_start_));
}

void* Arena::getMapStart() {
    return map_start_;
}

void* ArenaNVM::CalculateOffset(void* ptr) {
    return reinterpret_cast<void*>(reinterpret_cast<intptr_t>(ptr) - reinterpret_cast<intptr_t>(map_start_));
}

char* Arena::AllocateFallback(size_t bytes) {

    char *result = NULL;
    alloc_ptr_ = AllocateNewBlock(kBlockSize);
    alloc_bytes_remaining_ = kBlockSize;

    result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
}

char* Arena::AllocateAligned(size_t bytes) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        // AllocateFallback always returned aligned memory
        result = AllocateFallback(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
}

char* Arena::AllocateFallback_submemIndex(size_t bytes, int sub_mem_index) {

    char *result = NULL;

    skiplist_alloc_ptr_[sub_mem_index] = AllocateNewBlock(SKIPLIST_ALLOC_SIZE);
    skiplist_alloc_bytes_remaining_[sub_mem_index] = SKIPLIST_ALLOC_SIZE;

    result = skiplist_alloc_ptr_[sub_mem_index];
    skiplist_alloc_ptr_[sub_mem_index] += bytes;
    skiplist_alloc_bytes_remaining_[sub_mem_index] -= bytes;
    return result;
}

char* Arena::AllocateAligned_submemIndex(size_t bytes, int sub_mem_index) {
    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;
    if (needed <= skiplist_alloc_bytes_remaining_[sub_mem_index]) {
        result = skiplist_alloc_ptr_[sub_mem_index] + slop;
        skiplist_alloc_ptr_[sub_mem_index] += needed;
        skiplist_alloc_bytes_remaining_[sub_mem_index] -= needed;
    } else {
        // AllocateFallback always returned aligned memory
        result = AllocateFallback_submemIndex(bytes, sub_mem_index);
        skiplist_blocks[sub_mem_index].push_back(result);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
    char* result = NULL;
    result = new char[block_bytes];
    memory_usage_.NoBarrier_Store(
            reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
    return result;
}


#ifdef ENABLE_RECOVERY
ArenaNVM::ArenaNVM(long size, std::string *filename, bool recovery)
{
    //: memory_usage_(0)
    if (recovery) {
        mfile = *filename;
        map_start_ = (void *)AllocateNVMBlock(size);
        kSize = MEM_THRESH * size;
        nvmarena_ = true;
        alloc_bytes_remaining_ = *((size_t *)map_start_);
        alloc_ptr_ = (char *)map_start_ + (kSize - alloc_bytes_remaining_);
        map_end_ = 0;
        memory_usage_.NoBarrier_Store(
                reinterpret_cast<void *>(kSize - alloc_bytes_remaining_));
        allocation = true;
    }
    else {
        //memory_usage_=0;
        alloc_ptr_ = NULL;  // First allocation will allocate a block
        alloc_bytes_remaining_ = 0;
        map_start_ = map_end_ = 0;
        nvmarena_ = true;
        kSize = MEM_THRESH * size;
        mfile = *filename;
        fd = -1;
        allocation = false;
    }
    isDataLock = 0;

    long online_core;
    online_core = sysconf(_SC_NPROCESSORS_ONLN);
    if(online_core == -1){}
    else
        cores = online_core;
    percore_alloc_ptr_ = (char**)malloc(sizeof(char*) * online_core);
    percore_alloc_bytes_remaining_ = (size_t*)malloc(sizeof(size_t) * online_core);
    for(int i=0; i<online_core; i++) {
        percore_alloc_ptr_[i] = NULL;
        percore_alloc_bytes_remaining_[i] = 0;
    }
    sub_mem_bset = (std::atomic_bool*)malloc(sizeof(std::atomic_bool) * size / SUB_MEM_SIZE);
    sub_mem_count = size / SUB_MEM_SIZE;
    sub_immem_bset = (std::atomic_bool*)malloc(sizeof(std::atomic_bool) * size / SUB_MEM_SIZE);
    sub_immem_count = 0;
    in_trans_bset = (std::atomic_bool*)malloc(sizeof(std::atomic_bool) * size / SUB_MEM_SIZE);

    skiplist_blocks = new std::vector<char*>[size / SUB_MEM_SIZE];
    skiplist_alloc_ptr_ = (char**)malloc(sizeof(char*) * size / SUB_MEM_SIZE);
    skiplist_alloc_bytes_remaining_ = (size_t*)malloc(sizeof(size_t) * size / SUB_MEM_SIZE);

    for(int i=0; i<(size / SUB_MEM_SIZE); i++) {
        sub_mem_bset[i] = 0;
        sub_immem_bset[i] = 0;
        in_trans_bset[i] = 0;
        skiplist_alloc_ptr_[i] = NULL;
        skiplist_alloc_bytes_remaining_[i] = 0;
    }
}
#else
ArenaNVM::ArenaNVM()
{
    //: memory_usage_(0)
    //memory_usage_=0;
    alloc_ptr_ = NULL;  // First allocation will allocate a block
    alloc_bytes_remaining_ = 0;
    map_start_ = map_end_ = 0;
    nvmarena_ = true;
    kSize = kBlockSize;
    mfile = "";
}
#endif

size_t Arena::getAllocRem() {
    return alloc_bytes_remaining_;
}

void* ArenaNVM::getMapStart() {
    return map_start_;
}

int ArenaNVM::alloc_sub_mem(int cpu) {
    int i;
    for(i=0; i<sub_mem_count; i++) {
        if(!sub_mem_bset[i].load() && !sub_mem_bset[i].exchange(true))
            break;
    }
    if(i == sub_mem_count)
        return -1;
    percore_alloc_ptr_[cpu] = (char*)map_start_ + i * SUB_MEM_SIZE;
    percore_alloc_bytes_remaining_[cpu] = SUB_MEM_SIZE;
    return i;
}

int ArenaNVM::swap_sub_mem(int cpu) {
    int sub_mem = (percore_alloc_ptr_[cpu] - (char*)map_start_) / SUB_MEM_SIZE;
    sub_immem_bset[sub_mem].store(1);
    sub_immem_count++;
    percore_alloc_ptr_[cpu] = NULL;
    percore_alloc_bytes_remaining_[cpu] = 0;
    return alloc_sub_mem(cpu);
}

void ArenaNVM::reclaim_sub_mem(int cpu){
    if(cpu!=-1 && !percore_alloc_ptr_[cpu])
        return;

    if(cpu == -1){
        for(int i=0; i<cores; i++) {
            percore_alloc_ptr_[i] = NULL;
            percore_alloc_bytes_remaining_[i] = 0;
        }
        for(int i=0; i<sub_mem_count; i++) {
            sub_mem_bset[i].store(0);
        }
    }
    else{
        int sub_mem = (percore_alloc_ptr_[cpu] - (char*)map_start_) / SUB_MEM_SIZE;
        percore_alloc_ptr_[cpu] = NULL;
        percore_alloc_bytes_remaining_[cpu] = 0;
        sub_mem_bset[sub_mem].store(0);
    }
}

void ArenaNVM::setSubMemToImm(){
    long online_core;
    online_core = sysconf(_SC_NPROCESSORS_ONLN);
    for(int i=0; i<online_core; i++) {
        percore_alloc_ptr_[i] = NULL;
        percore_alloc_bytes_remaining_[i] = 0;
    }
    for(int i=0; i<sub_mem_count; i++) {
        sub_immem_bset[i].store(1);
        sub_immem_count++;
    }
}

int ArenaNVM::init_memory(char* mmap_ptr, size_t sz)
{
        size_t i;

        if (sz <= 0)
                return -1;

        if (mmap_ptr == NULL)
                return -1;

        for (i = 0; i < sz; i += 32)
                mmap_ptr[i] = (char)rand();

        return 1;
}

static void mem_flush(const void *p, const size_t s);
int ArenaNVM::dlock_exit(void)
{
    int ret = 0;
    unsigned i;

    for (i = 0; i < DIM(m_l3cat_cos); i++) {
        if (m_l3cat_cos[i].cos_tab != NULL) {
            int res = pqos_l3ca_set(m_l3cat_cos[i].id, m_num_clos, m_l3cat_cos[i].cos_tab);
            if (res != PQOS_RETVAL_OK)
                ret = -2;
        }
        free(m_l3cat_cos[i].cos_tab);
        m_l3cat_cos[i].cos_tab = NULL;
        m_l3cat_cos[i].id = 0;
    }
    for (size_t i = 0; i < blocks_.size(); i++) {
        mem_flush(blocks_[i], kSize);
    }

    return ret;
}

void* ArenaNVM:: operator new(size_t size)
{
#ifdef _USE_ARENA2_ALLOC
    return xxmalloc(size);
#else
    return malloc(size);
#endif
}

void* ArenaNVM::operator new[](size_t size) {
#ifdef _USE_ARENA2_ALLOC
    return xxmalloc(size);
#else
    return malloc(size);
#endif
}

ArenaNVM::~ArenaNVM() {
    for (size_t i = 0; i < blocks_.size(); i++) {
#ifdef ENABLE_RECOVERY
        munmap(blocks_[i], kSize);
        blocks_[i] = NULL;
    }
    close(fd);
#else
        delete[] blocks_[i];
        blocks_[i] = NULL;
    }
#endif
    if(isDataLock)
        dlock_exit();
    free(percore_alloc_ptr_);
    free(percore_alloc_bytes_remaining_);
    free(sub_mem_bset);
    free(sub_immem_bset);
    free(in_trans_bset);
    for (size_t i = 0; i < sub_mem_count; i++) {
        for(size_t j = 0; j<skiplist_blocks[i].size(); j++) {
            delete[] skiplist_blocks[i][j];
            skiplist_blocks[i][j] = NULL;
        }
    }
    free(skiplist_alloc_ptr_);
    free(skiplist_alloc_bytes_remaining_);
}

void ArenaNVM::operator delete(void* ptr)
{
#ifdef _USE_ARENA2_ALLOC
    xxfree(ptr);
#else
    delete[] (char*)ptr;
#endif
    ptr = NULL;
}

static void
mem_flush(const void *p, const size_t s)
{
    const size_t cache_line = 64;
    const char *cp = (const char *)p;
    size_t i = 0;

    if (p == NULL || s <= 0)
        return;

    for (i = 0; i < s; i += cache_line)
        asm volatile("clflush (%0)\n\t" : : "r"(&cp[i]) : "memory");

    asm volatile("sfence\n\t" : : : "memory");
}

static void
mem_read(const void *p, const size_t s)
{
    register size_t i;

    if (p == NULL || s <= 0)
        return;

    for (i = 0; i < (s / sizeof(uint32_t)); i++) {
        asm volatile("xor (%0,%1,4), %%eax\n\t"
                     :
                     : "r"(p), "r"(i)
                     : "%eax", "memory");
    }

    for (i = s & (~(sizeof(uint32_t) - 1)); i < s; i++) {
        asm volatile("xorb (%0,%1,1), %%al\n\t"
                     :
                     : "r"(p), "r"(i)
                     : "%al", "memory");
    }
}

int
dlock_init(void *ptr, const size_t way_count, const size_t dlock_size, const int clos, const int cpuid)
{
	int ret = 0, res = 0, i = 0;
	unsigned *l3cat_ids = NULL;
	unsigned l3cat_id_count = 0;
	const struct pqos_capability *p_l3ca_cap = NULL;
    unsigned clos_save = 0;
	size_t num_cache_ways = way_count;//16MB

    cpu_set_t cpuset_save, cpuset;
    res = sched_getaffinity(0, sizeof(cpuset_save), &cpuset_save);
    if (res != 0) {
        printf("cpu sched get err\n");
        ret = -4;
        return ret;
    }

    CPU_ZERO(&cpuset);
    CPU_SET(cpuid, &cpuset);
    res = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    if (res != 0) {
        printf("cpu sched set err\n");
        ret = -4;
        return ret;
    }

	for (i = 0; i < DIM(m_l3cat_cos); i++) {
    	m_l3cat_cos[i].id = 0;
        m_l3cat_cos[i].cos_tab = NULL;
    }
	
	res = pqos_cap_get(&g_p_cap, &g_p_cpu);
	if(res != PQOS_RETVAL_OK) {
        printf("cap get err\n");
		goto dlock_init_error1;
	}

	l3cat_ids = pqos_cpu_get_l3cat_ids(g_p_cpu, &l3cat_id_count);
    if (l3cat_ids == NULL) {
        ret = -6;
        printf("l3cat_ids get err\n");
    }
	
	res = pqos_cap_get_type(g_p_cap, PQOS_CAP_TYPE_L3CA, &p_l3ca_cap);
	if (res != PQOS_RETVAL_OK) {
        printf("cap get type err res=%d\n", res);
        ret = -7;
    }

	m_num_clos = p_l3ca_cap->u.l3ca->num_classes;
	for (i = 0; i < l3cat_id_count; i++) {
		const uint64_t dlock_mask = (1ULL << num_cache_ways) - 1ULL;
		struct pqos_l3ca cos[m_num_clos];
		unsigned num = 0, j;
		res = pqos_l3ca_get(l3cat_ids[i], m_num_clos, &num, &cos[0]);
		if (res != PQOS_RETVAL_OK) {
            printf("pqos_l3ca_get() error!\n");
			ret = -9;
			goto dlock_init_error2;
		}
		if (m_num_clos != num) {
            printf("CLOS number mismatch!\n");
			ret = -9;
			goto dlock_init_error2;
		}

		m_l3cat_cos[i].id = l3cat_ids[i];
        m_l3cat_cos[i].cos_tab = malloc(m_num_clos * sizeof(cos[0]));
		if (m_l3cat_cos[i].cos_tab == NULL) {
            printf("malloc() error!\n");
            ret = -9;
            goto dlock_init_error2;
        }
		memcpy(m_l3cat_cos[i].cos_tab, cos, m_num_clos * sizeof(cos[0]));
		for (j = 0; j < m_num_clos; j++) {
            if (cos[j].cdp) {
                if (cos[j].class_id == (unsigned)clos) {
                    cos[j].u.s.code_mask = dlock_mask;
                    cos[j].u.s.data_mask = dlock_mask;
                } else {
                    cos[j].u.s.code_mask &= ~dlock_mask;
                    cos[j].u.s.data_mask &= ~dlock_mask;
                }
            } else {
                if (cos[j].class_id == (unsigned)clos)
                    cos[j].u.ways_mask = dlock_mask;
                else
                    cos[j].u.ways_mask &= ~dlock_mask;
            }
        }
		res = pqos_l3ca_set(l3cat_ids[i], m_num_clos, &cos[0]);
        if (res != PQOS_RETVAL_OK) {
            printf("pqos_l3ca_set() error!\n");
            ret = -10;
            goto dlock_init_error2;
        }
	}
	res = pqos_alloc_assoc_get(cpuid, &clos_save);
    if (res != PQOS_RETVAL_OK) {
        printf("pqos_alloc_assoc_get() error!\n");
        ret = -11;
        goto dlock_init_error2;
    }
    res = pqos_alloc_assoc_set(cpuid, clos);
    if (res != PQOS_RETVAL_OK) {
        printf("pqos_alloc_assoc_get() error!\n");
        ret = -12;
        goto dlock_init_error2;
    }
	mem_flush(ptr, dlock_size);
	for (i = 0; i < 10; i++)
        mem_read(ptr, dlock_size);

	res = pqos_alloc_assoc_set(cpuid, clos_save);
    if (res != PQOS_RETVAL_OK) {
        printf("pqos_alloc_assoc_set() error (revert)!\n");
        ret = -13;
        goto dlock_init_error2;
    }

dlock_init_error2:
    for (i = 0; (i < DIM(m_l3cat_cos)) && (ret != 0); i++)
        if (m_l3cat_cos[i].cos_tab != NULL)
            free(m_l3cat_cos[i].cos_tab);

    res = sched_setaffinity(0, sizeof(cpuset_save), &cpuset_save);
    if (res != 0) {
        printf("cpu sched restore err\n");
    }

dlock_init_error1:
    if (l3cat_ids != NULL)
        free(l3cat_ids);
    return ret;
}

char* ArenaNVM::AllocateNVMBlock(size_t block_bytes) {
    //NoveLSM
#ifdef ENABLE_RECOVERY
    fd = open(mfile.c_str(), O_RDWR);
    if (fd == -1) {
        fd = open(mfile.c_str(), O_RDWR | O_CREAT, 0664);
        if (fd < 0) {
            return NULL;
        }
    }

    if(ftruncate(fd, block_bytes) != 0){
        perror("ftruncate failed \n");
        return NULL;
    }

    char* result = (char *)mmap(NULL, block_bytes, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

    if(isDataLock) {
        init_memory(result, block_bytes);
        if(dlock_init(result, dlock_way, dlock_size, 1, 0))
            printf("dlock err\n");
    }

    allocation = true;
    blocks_.push_back(result);
    assert(blocks_.size() <= 1);
#else
    char* result = new char[block_bytes];
    blocks_.push_back(result);
#endif
    return result;
}

char* ArenaNVM::AllocateFallbackNVM(size_t bytes) {
    char *tmp_ptr;
    if(isDataLock)
        tmp_ptr = AllocateNVMBlock(kSize);
    else
        tmp_ptr = AllocateNVMBlock(SUB_MEM_SIZE);
    map_start_ = (void *)tmp_ptr;

#if defined(ENABLE_RECOVERY)
    memory_usage_.NoBarrier_Store(
            reinterpret_cast<void*>(MemoryUsage() + bytes + sizeof(char*)));
#else
    memory_usage_.NoBarrier_Store(
            reinterpret_cast<void*>(MemoryUsage() + kSize + sizeof(char*)));
#endif
    return tmp_ptr;
}

char* ArenaNVM::AllocateAlignedNVM(size_t bytes) {

    const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
    assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char* result;

#if defined(ENABLE_RECOVERY)
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
        memory_usage_.NoBarrier_Store(
                reinterpret_cast<void*>(MemoryUsage() + needed + sizeof(char*)));
    } else {
        if (allocation) {
            alloc_bytes_remaining_ = 0;
            result = alloc_ptr_ + slop;
            alloc_ptr_ += needed;
            memory_usage_.NoBarrier_Store(
                    reinterpret_cast<void*>(MemoryUsage() + needed + sizeof(char*)));
        } else {
            result = this->AllocateFallbackNVM(bytes);
        }
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
#else
    if (needed <= alloc_bytes_remaining_) {
        result = alloc_ptr_ + slop;
        alloc_ptr_ += needed;
        alloc_bytes_remaining_ -= needed;
    } else {
        result = this->AllocateFallbackNVM(bytes);
    }
    assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
    return result;
#endif
}

//TODO: This method just implements virtual function
char* ArenaNVM::AllocateAligned(size_t bytes) {
    return NULL;
}

}  // namespace leveldb
