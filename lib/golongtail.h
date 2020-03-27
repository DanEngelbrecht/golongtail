#define _GNU_SOURCE
#include "import/src/longtail.h"
#include "import/lib/bikeshed/longtail_bikeshed.h"
#include "import/lib/blake2/longtail_blake2.h"
#include "import/lib/blake3/longtail_blake3.h"
#include "import/lib/brotli/longtail_brotli.h"
#include "import/lib/cacheblockstore/longtail_cacheblockstore.h"
#include "import/lib/compressblockstore/longtail_compressblockstore.h"
#include "import/lib/compressionregistry/longtail_full_compression_registry.h"
#include "import/lib/filestorage/longtail_filestorage.h"
#include "import/lib/fsblockstore/longtail_fsblockstore.h"
#include "import/lib/lz4/longtail_lz4.h"
#include "import/lib/memstorage/longtail_memstorage.h"
#include "import/lib/meowhash/longtail_meowhash.h"
#include "import/lib/zstd/longtail_zstd.h"
#include <stdlib.h>
#include <string.h>
#include <errno.h>

//void progressProxy(void* context, uint32_t total_count, uint32_t done_count);

static void* OffsetPointer(void* pointer, size_t offset)
{
    return &((uint8_t*)pointer)[offset];
}

static int DisposeStoredBlockFromRaw(struct Longtail_StoredBlock* stored_block)
{
    Longtail_Free((void*)stored_block);
    return 0;
}

static int CreateStoredBlockFromRaw(
    void* data,
    size_t data_size,
    struct Longtail_StoredBlock** out_stored_block)
{
    size_t stored_block_size = Longtail_GetStoredBlockSize(data_size);
    void* block_data = Longtail_Alloc(stored_block_size);
    void* rawBlockDataBuffer = OffsetPointer(block_data, stored_block_size-data_size);
    memmove(rawBlockDataBuffer, data, data_size);
    struct Longtail_StoredBlock* stored_block = (struct Longtail_StoredBlock*)block_data;
    int err = Longtail_InitStoredBlockFromData(
        stored_block,
        data,
        data_size);
    if (err)
    {
        Longtail_Free(block_data);
        return err;
    }
    stored_block->Dispose = DisposeStoredBlockFromRaw;
    *out_stored_block = stored_block;
    return 0;
}

void Proxy_BlockStore_Dispose(void* context);
int Proxy_PutStoredBlock(void* context, struct Longtail_StoredBlock* stored_block, struct Longtail_AsyncPutStoredBlockAPI* async_complete_api);
int Proxy_GetStoredBlock(void* context, uint64_t block_hash, struct Longtail_AsyncGetStoredBlockAPI* async_complete_api);
int Proxy_GetIndex(void* context, struct Longtail_JobAPI* job_api, uint32_t default_hash_api_identifier, struct Longtail_ProgressAPI* progressAPI, struct Longtail_AsyncGetIndexAPI* async_complete_api);
void Proxy_Close(void* context);

struct BlockStoreAPIProxy
{
    struct Longtail_BlockStoreAPI m_API;
    void* m_Context;
};

static void BlockStoreAPIProxy_Dispose(struct Longtail_API* block_store_api)
{
    struct BlockStoreAPIProxy* proxy = (struct BlockStoreAPIProxy*)block_store_api;
    Proxy_Close(proxy->m_Context);
    Longtail_Free(proxy);
}

static int BlockStoreAPIProxy_PutStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_StoredBlock* stored_block,
    struct Longtail_AsyncPutStoredBlockAPI* async_complete_api)
{
    struct BlockStoreAPIProxy* proxy = (struct BlockStoreAPIProxy*)block_store_api;
    return Proxy_PutStoredBlock(proxy->m_Context, stored_block, async_complete_api);
}

static int BlockStoreAPIProxy_GetStoredBlock(
    struct Longtail_BlockStoreAPI* block_store_api,
    uint64_t block_hash,
    struct Longtail_AsyncGetStoredBlockAPI* async_complete_api)
{
    struct BlockStoreAPIProxy* proxy = (struct BlockStoreAPIProxy*)block_store_api;
    return Proxy_GetStoredBlock(proxy->m_Context, block_hash, async_complete_api);
}

static int BlockStoreAPIProxy_GetIndex(
    struct Longtail_BlockStoreAPI* block_store_api,
    struct Longtail_JobAPI* job_api,
    uint32_t default_hash_api_identifier,
    struct Longtail_ProgressAPI* progress_api,
    struct Longtail_AsyncGetIndexAPI* async_complete_api)
{
    struct BlockStoreAPIProxy* proxy = (struct BlockStoreAPIProxy*)block_store_api;
    return Proxy_GetIndex(proxy->m_Context, job_api, default_hash_api_identifier, progress_api, async_complete_api);
}

static int AsyncComplete_OnPutBlockComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    if (async_complete_api)
    {
        return async_complete_api->OnComplete(async_complete_api, err);
    }
    return err;
}

static int AsyncComplete_OnGetBlockComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    if (async_complete_api)
    {
        return async_complete_api->OnComplete(async_complete_api, stored_block, err);
    }
    return err;
}

static int AsyncComplete_OnGetIndexComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    if (async_complete_api)
    {
        return async_complete_api->OnComplete(async_complete_api, content_index, err);
    }
    return err;
}

static struct Longtail_BlockStoreAPI* CreateBlockStoreProxyAPI(void* context)
{
    struct BlockStoreAPIProxy* api = (struct BlockStoreAPIProxy*)Longtail_Alloc(sizeof(struct BlockStoreAPIProxy));
    api->m_API.m_API.Dispose        = BlockStoreAPIProxy_Dispose;
    api->m_API.PutStoredBlock       = BlockStoreAPIProxy_PutStoredBlock;
    api->m_API.GetStoredBlock       = BlockStoreAPIProxy_GetStoredBlock;
    api->m_API.GetIndex             = BlockStoreAPIProxy_GetIndex;
    api->m_Context = context;
    return &api->m_API;
}

struct ProgressAPIProxy
{
    struct Longtail_ProgressAPI m_API;
    void* m_Context;
};

void ProgressAPIProxyOnProgress(void* context, uint32_t total_count, uint32_t done_count);

static void ProgressAPIProxy_OnProgress(struct Longtail_ProgressAPI* progress_api, uint32_t total_count, uint32_t done_count)
{
    struct ProgressAPIProxy* proxy = (struct ProgressAPIProxy*)progress_api;
    ProgressAPIProxyOnProgress(proxy->m_Context, total_count, done_count);
}

static void ProgressAPIProxy_Dispose(struct Longtail_API* api)
{
    struct ProgressAPIProxy* proxy = (struct ProgressAPIProxy*)api;
    Longtail_Free(proxy);
}

static struct Longtail_ProgressAPI* CreateProgressProxyAPI(void* context)
{
    struct ProgressAPIProxy* api    = (struct ProgressAPIProxy*)Longtail_Alloc(sizeof(struct ProgressAPIProxy));
    api->m_API.m_API.Dispose        = ProgressAPIProxy_Dispose;
    api->m_API.OnProgress           = ProgressAPIProxy_OnProgress;
    api->m_Context = context;
    return &api->m_API;
}

////////////// Longtail_AsyncPutStoredBlockAPI

struct AsyncPutStoredBlockAPIProxy
{
    struct Longtail_AsyncPutStoredBlockAPI m_API;
    void* m_Context;
};

int AsyncPutStoredBlockAPIProxyOnComplete(void* context, int err);

static int AsyncPutStoredBlockAPIProxy_OnComplete(struct Longtail_AsyncPutStoredBlockAPI* async_complete_api, int err)
{
    struct AsyncPutStoredBlockAPIProxy* proxy = (struct AsyncPutStoredBlockAPIProxy*)async_complete_api;
    return AsyncPutStoredBlockAPIProxyOnComplete(proxy->m_Context, err);
}

static void AsyncPutStoredBlockAPIProxy_Dispose(struct Longtail_API* api)
{
    struct AsyncPutStoredBlockAPIProxy* proxy = (struct AsyncPutStoredBlockAPIProxy*)api;
    Longtail_Free(proxy);
}

static struct Longtail_AsyncPutStoredBlockAPI* CreateAsyncPutStoredBlockAPI(void* context)
{
    struct AsyncPutStoredBlockAPIProxy* api    = (struct AsyncPutStoredBlockAPIProxy*)Longtail_Alloc(sizeof(struct AsyncPutStoredBlockAPIProxy));
    api->m_API.m_API.Dispose        = AsyncPutStoredBlockAPIProxy_Dispose;
    api->m_API.OnComplete           = AsyncPutStoredBlockAPIProxy_OnComplete;
    api->m_Context = context;
    return &api->m_API;
}

////////////// Longtail_AsyncGetStoredBlockAPI

struct AsyncGetStoredBlockAPIProxy
{
    struct Longtail_AsyncGetStoredBlockAPI m_API;
    void* m_Context;
};

int AsyncGetStoredBlockAPIProxyOnComplete(void* context, struct Longtail_StoredBlock* stored_block, int err);

static int AsyncGetStoredBlockAPIProxy_OnComplete(struct Longtail_AsyncGetStoredBlockAPI* async_complete_api, struct Longtail_StoredBlock* stored_block, int err)
{
    struct AsyncGetStoredBlockAPIProxy* proxy = (struct AsyncGetStoredBlockAPIProxy*)async_complete_api;
    return AsyncGetStoredBlockAPIProxyOnComplete(proxy->m_Context, stored_block, err);
}

static void AsyncGetStoredBlockAPIProxy_Dispose(struct Longtail_API* api)
{
    struct AsyncGetStoredBlockAPIProxy* proxy = (struct AsyncGetStoredBlockAPIProxy*)api;
    Longtail_Free(proxy);
}

static struct Longtail_AsyncGetStoredBlockAPI* CreateAsyncGetStoredBlockAPI(void* context)
{
    struct AsyncGetStoredBlockAPIProxy* api    = (struct AsyncGetStoredBlockAPIProxy*)Longtail_Alloc(sizeof(struct AsyncGetStoredBlockAPIProxy));
    api->m_API.m_API.Dispose        = AsyncGetStoredBlockAPIProxy_Dispose;
    api->m_API.OnComplete           = AsyncGetStoredBlockAPIProxy_OnComplete;
    api->m_Context = context;
    return &api->m_API;
}

////////////// Longtail_AsyncGetIndexAPI

struct AsyncGetIndexAPIProxy
{
    struct Longtail_AsyncGetIndexAPI m_API;
    void* m_Context;
};

int AsyncGetIndexAPIProxyOnComplete(void* context, struct Longtail_ContentIndex* content_index, int err);

static int AsyncGetIndexAPIProxy_OnComplete(struct Longtail_AsyncGetIndexAPI* async_complete_api, struct Longtail_ContentIndex* content_index, int err)
{
    struct AsyncGetIndexAPIProxy* proxy = (struct AsyncGetIndexAPIProxy*)async_complete_api;
    return AsyncGetIndexAPIProxyOnComplete(proxy->m_Context, content_index, err);
}

static void AsyncGetIndexAPIProxy_Dispose(struct Longtail_API* api)
{
    struct AsyncGetIndexAPIProxy* proxy = (struct AsyncGetIndexAPIProxy*)api;
    Longtail_Free(proxy);
}

static struct Longtail_AsyncGetIndexAPI* CreateAsyncGetIndexAPI(void* context)
{
    struct AsyncGetIndexAPIProxy* api    = (struct AsyncGetIndexAPIProxy*)Longtail_Alloc(sizeof(struct AsyncGetIndexAPIProxy));
    api->m_API.m_API.Dispose        = AsyncGetIndexAPIProxy_Dispose;
    api->m_API.OnComplete           = AsyncGetIndexAPIProxy_OnComplete;
    api->m_Context = context;
    return &api->m_API;
}

void logProxy(void* context, int level, char* str);

void assertProxy(char* expression, char* file, int line);

static int Storage_Write(struct Longtail_StorageAPI* api, const char* path, uint64_t length, const void* input)
{
    Longtail_StorageAPI_HOpenFile f;
    int err = api->OpenWriteFile(api, path, length, &f);
    if (err)
    {
        return err;
    }
    err = api->Write(api, f, 0, length, input);
    api->CloseFile(api, f);
    return err;
}

static uint64_t Storage_GetSize(struct Longtail_StorageAPI* api, const char* path)
{
    Longtail_StorageAPI_HOpenFile f;
    int err = api->OpenReadFile(api, path, &f);
    if (err)
    {
        return err;
    }
    uint64_t s;
    err = api->GetSize(api, f, &s);
    api->CloseFile(api, f);
    return err ? 0 : s;
}

static int Storage_Read(struct Longtail_StorageAPI* api, const char* path, uint64_t offset, uint64_t length, void* output)
{
    Longtail_StorageAPI_HOpenFile f;
    int err = api->OpenReadFile(api, path, &f);
    if (err)
    {
        return err;
    }
    err = api->Read(api, f, offset, length, output);
    api->CloseFile(api, f);
    return err;
}

static const char* GetPath(const uint32_t* name_offsets, const char* name_data, uint32_t index)
{
    return &name_data[name_offsets[index]];
}
