/* BlockMerchant - main.c */

#define NBDKIT_API_VERSION 2
#include <nbdkit-plugin.h>
#include <stddef.h>

#define THREAD_MODEL NBDKIT_THREAD_MODEL_SERIALIZE_ALL_REQUESTS

/* Open a device handle. */
static void* bm_open(int read_only){
    /* To be completed. */
    return NULL;
}

/* Close a device handle.*/
static void bm_close(void* handle){
    /* To be completed. */
    //return 0;
}

/* Return the size of the block device. */
static int64_t bm_get_size(void* handle){
    return 0;
}

/* Read a block to a buffer. */
static int bm_pread(void *handle, void *buf, uint32_t count, uint64_t offset, uint32_t flags){
    return 0;
}

/* Write a block from a buffer.*/
static int bm_pwrite(void *handle, const void *buf, uint32_t count, uint64_t offset, uint32_t flags){
    return 0;
}

static struct nbdkit_plugin plugin = {
    .name              = "MessageDiskClient",
    .open              = bm_open,
    .close             = bm_close,
    .get_size          = bm_get_size,
    .pread             = bm_pread,
    .pwrite            = bm_pwrite,
};

NBDKIT_REGISTER_PLUGIN(plugin)
