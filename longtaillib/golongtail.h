#ifndef _GNU_SOURCE
    #define _GNU_SOURCE
#endif
#include "longtail/src/longtail.h"
#include "longtail/lib/bikeshed/longtail_bikeshed.h"
#include "longtail/lib/blake2/longtail_blake2.h"
#include "longtail/lib/blake3/longtail_blake3.h"
#include "longtail/lib/brotli/longtail_brotli.h"
#include "longtail/lib/cacheblockstore/longtail_cacheblockstore.h"
#include "longtail/lib/compressblockstore/longtail_compressblockstore.h"
#include "longtail/lib/compressionregistry/longtail_full_compression_registry.h"
#include "longtail/lib/filestorage/longtail_filestorage.h"
#include "longtail/lib/fsblockstore/longtail_fsblockstore.h"
#include "longtail/lib/lz4/longtail_lz4.h"
#include "longtail/lib/memstorage/longtail_memstorage.h"
#include "longtail/lib/meowhash/longtail_meowhash.h"
#include "longtail/lib/zstd/longtail_zstd.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

void LogProxy_Log(void* context, int level, char* str);

void AssertProxy_Assert(char* expression, char* file, int line);

////////////// Longtail_BlockStoreAPI

struct BlockStoreAPIProxy
{
    struct Longtail_BlockStoreAPI m_API;
    void* m_Context;
};

static void* BlockStoreAPIProxy_GetContext(void* api) { return ((struct BlockStoreAPIProxy*)api)->m_Context; }
void BlockStoreAPIProxy_Dispose(struct Longtail_API* api);
int BlockStoreAPIProxy_PutStoredBlock(struct Longtail_BlockStoreAPI* api, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api);
int BlockStoreAPIProxy_GetStoredBlock(struct Longtail_BlockStoreAPI* api, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api);
int BlockStoreAPIProxy_GetIndex(struct Longtail_BlockStoreAPI* api, uint32_t default_hash_api_identifier, struct Longtail_AsyncGetIndexAPI* async_complete_api);
int BlockStoreAPIProxy_GetStats(struct Longtail_BlockStoreAPI* api, struct Longtail_BlockStore_Stats* out_stats);

static struct Longtail_BlockStoreAPI* CreateBlockStoreProxyAPI(void* context)
{
    struct BlockStoreAPIProxy* api = (struct BlockStoreAPIProxy*)Longtail_Alloc(sizeof(struct BlockStoreAPIProxy));
    api->m_Context = context;
    return Longtail_MakeBlockStoreAPI(
        api,
        BlockStoreAPIProxy_Dispose,
        BlockStoreAPIProxy_PutStoredBlock,
        BlockStoreAPIProxy_GetStoredBlock,
        BlockStoreAPIProxy_GetIndex,
        BlockStoreAPIProxy_GetStats);
}

////////////// Longtail_ProgressAPI

struct ProgressAPIProxy
{
    struct Longtail_ProgressAPI m_API;
    void* m_Context;
};

static void* ProgressAPIProxy_GetContext(void* api) { return ((struct ProgressAPIProxy*)api)->m_Context; }
void ProgressAPIProxy_Dispose(struct Longtail_API* api);
void ProgressAPIProxy_OnProgress(struct Longtail_ProgressAPI* progress_api, uint32_t total_count, uint32_t done_count);

static struct Longtail_ProgressAPI* CreateProgressProxyAPI(void* context)
{
    struct ProgressAPIProxy* api = (struct ProgressAPIProxy*)Longtail_Alloc(sizeof(struct ProgressAPIProxy));
    api->m_Context = context;
    return Longtail_MakeProgressAPI(
        api,
        ProgressAPIProxy_Dispose,
        ProgressAPIProxy_OnProgress);
}

////////////// Longtail_AsyncPutStoredBlockAPI

struct AsyncPutStoredBlockAPIProxy
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    void* m_Context;
};

static void* AsyncPutStoredBlockAPIProxy_GetContext(void* api) { return ((struct AsyncPutStoredBlockAPIProxy*)api)->m_Context; }
int AsyncPutStoredBlockAPIProxy_OnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err);
void AsyncPutStoredBlockAPIProxy_Dispose(struct Longtail_API* api);

static struct Longtail_AsyncPutStoredBlockAPI* CreateAsyncPutStoredBlockAPI(void* context)
{
    struct AsyncPutStoredBlockAPIProxy* api = (struct AsyncPutStoredBlockAPIProxy*)Longtail_Alloc(sizeof(struct AsyncPutStoredBlockAPIProxy));
    api->m_Context = context;
    return Longtail_MakeAsyncPutStoredBlockAPI(
        api,
        AsyncPutStoredBlockAPIProxy_Dispose,
        AsyncPutStoredBlockAPIProxy_OnComplete);
}

////////////// Longtail_AsyncGetStoredBlockAPI

struct AsyncGetStoredBlockAPIProxy
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    void* m_Context;
};

static void* AsyncGetStoredBlockAPIProxy_GetContext(void* api) { return ((struct AsyncGetStoredBlockAPIProxy*)api)->m_Context; }
int AsyncGetStoredBlockAPIProxy_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err);
void AsyncGetStoredBlockAPIProxy_Dispose(struct Longtail_API* api);

static struct Longtail_AsyncGetStoredBlockAPI* CreateAsyncGetStoredBlockAPI(void* context)
{
    struct AsyncGetStoredBlockAPIProxy* api = (struct AsyncGetStoredBlockAPIProxy*)Longtail_Alloc(sizeof(struct AsyncGetStoredBlockAPIProxy));
    api->m_Context = context;
    return Longtail_MakeAsyncGetStoredBlockAPI(
        api,
        AsyncGetStoredBlockAPIProxy_Dispose,
        AsyncGetStoredBlockAPIProxy_OnComplete);
}

////////////// Longtail_AsyncGetIndexAPI

struct AsyncGetIndexAPIProxy
{
    struct Longtail_AsyncGetIndexAPI m_API;
    void* m_Context;
};

static void* AsyncGetIndexAPIProxy_GetContext(void* api) { return ((struct AsyncGetIndexAPIProxy*)api)->m_Context; }
int AsyncGetIndexAPIProxy_OnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err);
void AsyncGetIndexAPIProxy_Dispose(struct Longtail_API* api);

static struct Longtail_AsyncGetIndexAPI* CreateAsyncGetIndexAPI(void* context)
{
    struct AsyncGetIndexAPIProxy* api    = (struct AsyncGetIndexAPIProxy*)Longtail_Alloc(sizeof(struct AsyncGetIndexAPIProxy));
    api->m_Context = context;
    return Longtail_MakeAsyncGetIndexAPI(
        api,
        AsyncGetIndexAPIProxy_Dispose,
        AsyncGetIndexAPIProxy_OnComplete);
}