#define _GNU_SOURCE
#include "import/src/longtail.h"
#include "import/lib/bikeshed/longtail_bikeshed.h"
#include "import/lib/blake2/longtail_blake2.h"
#include "import/lib/brotli/longtail_brotli.h"
#include "import/lib/filestorage/longtail_filestorage.h"
#include "import/lib/lizard/longtail_lizard.h"
#include "import/lib/memstorage/longtail_memstorage.h"
#include "import/lib/meowhash/longtail_meowhash.h"
#include <stdlib.h>

void progressProxy(void* context, uint32_t total_count, uint32_t done_count);

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

static char* Storage_ConcatPath(struct Longtail_StorageAPI* api, const char* root_path, const char* sub_path)
{
    return api->ConcatPath(api, root_path, sub_path);
}

static const char* GetPath(const uint32_t* name_offsets, const char* name_data, uint32_t index)
{
    return &name_data[name_offsets[index]];
}

static struct Longtail_CompressionRegistryAPI* CompressionRegistry_CreateDefault()
{
    struct Longtail_CompressionAPI* lizard_compression = Longtail_CreateLizardCompressionAPI();
    if (lizard_compression == 0)
    {
        return 0;
    }
    Longtail_CompressionAPI_HSettings lizard_settings = lizard_compression->GetDefaultSettings(lizard_compression);

    struct Longtail_CompressionAPI* brotli_compression = Longtail_CreateBrotliCompressionAPI();
    if (brotli_compression == 0)
    {
        return 0;
    }
    Longtail_CompressionAPI_HSettings brotli_settings = brotli_compression->GetDefaultSettings(brotli_compression);

    uint32_t compression_types[2] = {LONGTAIL_LIZARD_DEFAULT_COMPRESSION_TYPE, LONGTAIL_BROTLI_DEFAULT_COMPRESSION_TYPE};
    struct Longtail_CompressionAPI* compression_apis[2] = {lizard_compression, brotli_compression};
    Longtail_CompressionAPI_HSettings compression_settings[2] = {lizard_settings, brotli_settings};

    struct Longtail_CompressionRegistryAPI* registry = Longtail_CreateDefaultCompressionRegistry(
        2,
        (const uint32_t*)compression_types,
        (const struct Longtail_CompressionAPI **)compression_apis,
        (const Longtail_CompressionAPI_HSettings*)compression_settings);
    if (registry == 0)
    {
        SAFE_DISPOSE_API(lizard_compression);
        return 0;
    }
    return registry;
}

static uint32_t GetBlake2HashIdentifier()
{
    return LONGTAIL_BLAKE2_HASH_TYPE;
}

static uint32_t GetMeowHashIdentifier()
{
    return LONGTAIL_MEOW_HASH_TYPE;
}
