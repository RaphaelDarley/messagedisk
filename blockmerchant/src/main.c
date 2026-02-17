/* BlockMerchant - main.c */

#define NBDKIT_API_VERSION 2

#define RING_ID 1
#define BLOCK_SIZE 512
#define BLOCK_COUNT 2048
#define DEVICE_SIZE (BLOCK_COUNT * BLOCK_SIZE)

#include <nbdkit-plugin.h>
#include <curl/curl.h>

#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <assert.h>

#define THREAD_MODEL NBDKIT_THREAD_MODEL_SERIALIZE_ALL_REQUESTS

const char* readUrl = "http://127.0.0.1:6767/read";
const char* writeUrl = "http://127.0.0.1:6767/write";

const char* startUrl = "http://127.0.0.1:6767/start";
const char* target = "127.0.0.1:6767";

int init_done = 0;
int currentRing = 1;

char* currentWriteTarget;
int loc;

typedef struct OpenRing {
    uint64_t ringId;
} OpenRing;

/* Open a device handle. */
static void* bm_open(int read_only){
    /* To be completed. */
    if(init_done == 0){
        CURLcode result = curl_global_init(CURL_GLOBAL_ALL);
        assert(result == 0);

        /* Send the request to start the ring. */
        CURL* req = curl_easy_init();

        curl_easy_setopt(req, CURLOPT_URL, startUrl);

        /* Make our JSON packet. */
        int msgSize = snprintf(NULL, 0, "{\"ring_id\": %llu, \"target\": \"%s\", \"chunk_num\": %d}", (uint64_t)RING_ID, target, BLOCK_COUNT);
        char* msg = malloc(msgSize);
        sprintf(msg, "{\"ring_id\": %llu, \"target\": \"%s\", \"chunk_num\": %d}", (uint64_t)RING_ID, target, BLOCK_COUNT);

        curl_easy_setopt(req, CURLOPT_POSTFIELDS, msg);

        /* Set the MIME type. */
        struct curl_slist* headers = curl_slist_append(NULL, "Content-Type: application/json");
        curl_easy_setopt(req, CURLOPT_HTTPHEADER, headers);

        /* Run the request. */
        result = curl_easy_perform(req);
        if(result != CURLE_OK){
            nbdkit_error("POST request to start ring failed");
            nbdkit_set_error(1);
            return -1;
        }
        free(msg);

        curl_easy_cleanup(req);

        init_done = 1;
    }

    /* Start a new ring. */
    OpenRing* ring = malloc(sizeof(OpenRing));
    ring->ringId = RING_ID;

    return ring;
}
 
/* Close a device handle.*/
static void bm_close(void* handle){
    /* To be completed. */
}

/* Return the size of the block device. */
static int64_t bm_get_size(void* handle){
    return DEVICE_SIZE;
}

typedef struct CurlWrite {
    char* buffer;
    int progress;
} CurlWrite;

static size_t on_curl_write(char* ptr, size_t size, size_t nmemb, void* userdata){
    CurlWrite* writeData = (CurlWrite*) userdata;

    memcpy(writeData->buffer + writeData->progress, ptr, nmemb);
    writeData->progress += nmemb;

    return nmemb;
}

/* Read a block to a buffer. */
static int bm_pread(void *handle, void *buf, uint32_t count, uint64_t offset, uint32_t flags){
    OpenRing* ring = (OpenRing*) handle;

    uint64_t chunkId = offset / BLOCK_SIZE;
    uint64_t chunkOffset = offset % BLOCK_SIZE;

    int countRemaining = count;
    int currentPoint = 0;

    while(countRemaining > 0){
        /* Make a request for a block. */
        CURLcode result;
        CURL* req = curl_easy_init();

        curl_easy_setopt(req, CURLOPT_URL, readUrl);

        /* Make our JSON packet. */
        int msgSize = snprintf(NULL, 0, "{\"ring_id\": %d, \"chunk_id\": %llu}", ring->ringId, chunkId);
        char* msg = malloc(msgSize + 1);
        sprintf(msg, "{\"ring_id\": %d, \"chunk_id\": %llu}", ring->ringId, chunkId);
        msg[msgSize] = 0;

        curl_easy_setopt(req, CURLOPT_POSTFIELDS, msg);

        /* Set the MIME type. */
        struct curl_slist* headers = curl_slist_append(NULL, "Content-Type: application/json");
        curl_easy_setopt(req, CURLOPT_HTTPHEADER, headers);

        /* Store the response. */
        char chunk[512];

        CurlWrite writeData;
        writeData.buffer = chunk;
        writeData.progress = 0;

        curl_easy_setopt(req, CURLOPT_WRITEFUNCTION, on_curl_write);
        curl_easy_setopt(req, CURLOPT_WRITEDATA, (void*)(&writeData));

        /* Run the request. */
        result = curl_easy_perform(req);
        if(result != CURLE_OK){
            nbdkit_error("POST request to read chunk %d failed", chunkId);
            nbdkit_set_error(1);
            return -1;
        }
        free(msg);

        /* Transpose the data. */
        int amountToCopy = (countRemaining > (BLOCK_SIZE - chunkOffset)) ? (BLOCK_SIZE - chunkOffset) : countRemaining;
        memcpy(buf + currentPoint, chunk + chunkOffset, amountToCopy);

        currentPoint += amountToCopy;
        countRemaining -= amountToCopy;
        chunkId++; chunkOffset = 0;

        curl_easy_cleanup(req);
    }

    return 0;
}

static char* createWriteMsg(uint64_t ringId, uint64_t chunkId, char* block){
    char* currentStr = NULL;
    int currentSize = 0;

    int msgSize = snprintf(NULL, 0, "{\"ring_id\": %llu, \"chunk_id\": %llu, \"data\":[%hhu", ringId, chunkId, block[0]);
    char* msg = malloc(msgSize);
    sprintf(msg, "{\"ring_id\": %llu, \"chunk_id\": %llu, \"data\":[%hhu", ringId, chunkId, block[0]);

    currentStr = msg;
    currentSize = msgSize;

    int i = 1;
    while(i < BLOCK_SIZE){
        int newMsgSize = currentSize + snprintf(NULL, 0, ",%hhu", block[i]);
        char* newMsg = malloc(newMsgSize); memcpy(newMsg, currentStr, currentSize);
        sprintf(newMsg + currentSize, ",%hhu", block[i]);

        free(currentStr);
        currentStr = newMsg;
        currentSize = newMsgSize;
        i++;
    }

    int newMsgSize = currentSize + 3;
    char* newMsg = malloc(newMsgSize); memcpy(newMsg, currentStr, currentSize);
    newMsg[newMsgSize - 3] = ']'; newMsg[newMsgSize - 2] = '}'; newMsg[newMsgSize - 1] = 0;

    free(currentStr);
    currentStr = newMsg;
    currentSize = newMsgSize;

    return currentStr;
}

/* Write a block from a buffer.*/
static int bm_pwrite(void *handle, const void *buf, uint32_t count, uint64_t offset, uint32_t flags){
    uint64_t chunkId = offset / BLOCK_SIZE;
    uint64_t chunkOffset = offset % BLOCK_SIZE;

    int countRemaining = count;
    int currentPoint = 0;

    while(countRemaining > 0){
        /* Get the current block. */
        char block[BLOCK_SIZE];
        bm_pread(NULL, block, BLOCK_SIZE, chunkId * BLOCK_SIZE, 0);

        int amountToCopy = (countRemaining > (BLOCK_SIZE - chunkOffset)) ? (BLOCK_SIZE - chunkOffset) : countRemaining;
        memcpy(block + chunkOffset, buf + currentPoint, amountToCopy);

        /* Make a request to write a block. */
        CURLcode result;
        CURL* req = curl_easy_init();

        curl_easy_setopt(req, CURLOPT_URL, writeUrl);

        /* Make our JSON packet. */
        char* msg = createWriteMsg(RING_ID, chunkId, block);

        curl_easy_setopt(req, CURLOPT_POSTFIELDS, msg);

        /* Set the MIME type. */
        struct curl_slist* headers = curl_slist_append(NULL, "Content-Type: application/json");
        curl_easy_setopt(req, CURLOPT_HTTPHEADER, headers);

        /* Run the request. */
        result = curl_easy_perform(req);
        if(result != CURLE_OK){
            nbdkit_error("POST request to write chunk %d failed", chunkId);
            nbdkit_set_error(1);
            return -1;
        }

        free(msg);

        curl_easy_cleanup(req);

        currentPoint += amountToCopy;
        countRemaining -= amountToCopy;
        chunkId++; chunkOffset = 0;
    }

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
