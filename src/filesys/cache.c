#include <debug.h>
#include <string.h>
#include "filesys/cache.h"
#include "filesys/filesys.h"
#include "threads/synch.h"

#define BUFFER_CACHE_SIZE 64

// A Queue Node (Queue is implemented using Doubly Linked List)
typedef struct buffer_cache_entry_t
{
  struct buffer_cache_entry_t *prev, *next;

  block_sector_t disk_sector;
  uint8_t buffer[BLOCK_SECTOR_SIZE];

  bool dirty; // dirty bit
} QNode;

// A Queue (A FIFO collection of Queue Nodes)
typedef struct Queue
{
  unsigned count;          // Number of filled sector
  unsigned numberOfSector; // total number of sector
  QNode *front, *rear;
} Queue;

// A hash (Collection of pointers to Queue Nodes)
typedef struct Hash
{
  int capacity;  // how many sector can be there
  QNode **array; // an array of queue nodes
} Hash;

QNode *newQNode(block_sector_t sector);
Queue *createQueue(int numberOfSector);
Hash *createHash(int capacity);
int AreAllSectorFull(Queue *queue);
int isQueueEmpty(Queue *queue);
void deQueue(Queue *queue);
uint32_t hashFun(Hash *hash, block_sector_t sectorNum);
void Enqueue(Queue *queue, Hash *hash, unsigned sectorNumber);
void ReferenceSector(Queue *queue, Hash *hash, unsigned sectorNumber);
void freeQueue(Queue *queue);
QNode *read_ahead(block_sector_t sector);

/* Buffer cache entries. */
static Queue *cache;
static Hash *hash;

// read head group size
static uint32_t read_head_group_size = 1;
// the sector block last time read
static block_sector_t last_read_sector;

/* A global lock for synchronizing buffer cache operations. */
static struct lock buffer_cache_lock;

void buffer_cache_init(void)
{
  lock_init(&buffer_cache_lock);
  cache = createQueue(BUFFER_CACHE_SIZE);
  hash = createHash(BUFFER_CACHE_SIZE);
}

/**
 * An internal method for flushing back the cache entry into disk.
 * Must be called with the lock held.
 */
static void
buffer_cache_flush(struct buffer_cache_entry_t *entry)
{
  ASSERT(lock_held_by_current_thread(&buffer_cache_lock));
  ASSERT(entry != NULL);

  if (entry->dirty)
  {
    block_write(fs_device, entry->disk_sector, entry->buffer);
    entry->dirty = false;
  }
}

void buffer_cache_close(void)
{
  // flush buffer cache entries
  lock_acquire(&buffer_cache_lock);

  size_t i;
  for (i = 0; i < hash->capacity; ++i)
  {
    if (hash->array[i]->dirty)
      buffer_cache_flush(hash->array[i]);
  }

  // free
  freeQueue(cache);
  free(hash->array);

  lock_release(&buffer_cache_lock);
}

/**
 * Lookup the cache entry, and returns the pointer of buffer_cache_entry_t,
 * or NULL in case of cache miss. (simply traverse the cache entries)
 */
static struct buffer_cache_entry_t *
buffer_cache_lookup(block_sector_t sector)
{
  // don't find
  if (hashFun(hash, sector) == -1)
  {
    return NULL;
  }

  return hash->array[hashFun(hash, sector)];
}

void buffer_cache_read(block_sector_t sector, void *target)
{
  lock_acquire(&buffer_cache_lock);
  QNode *slot = read_ahead(sector);
  // copy the buffer data into memory.
  memcpy(target, slot->buffer, BLOCK_SECTOR_SIZE);

  // read head
  // 同步预读
  if (slot->disk_sector > last_read_sector + read_head_group_size || slot->disk_sector < last_read_sector)
  {
    read_ahead(sector + 1);
    read_head_group_size = 1;
    last_read_sector = slot->disk_sector;
  }
  else
  {
    // 异步预读
    size_t i = 0;
    for (i = last_read_sector + read_head_group_size + 1; i <= last_read_sector + read_head_group_size + 3; i++)
    {
      read_ahead(i);
    }

    read_head_group_size = read_head_group_size + 3;
  }

  lock_release(&buffer_cache_lock);
}

QNode *read_ahead(block_sector_t sector)
{
  struct buffer_cache_entry_t *slot = buffer_cache_lookup(sector);
  if (slot == NULL)
  {
    // cache miss: need eviction.
    Enqueue(cache, hash, sector);
    slot = cache->front;
    ASSERT(slot != NULL);

    // fill in the cache entry.
    slot->dirty = false;
    block_read(fs_device, sector, slot->buffer);
  }

  return slot;
}

void buffer_cache_write(block_sector_t sector, const void *source)
{
  lock_acquire(&buffer_cache_lock);

  struct buffer_cache_entry_t *slot = buffer_cache_lookup(sector);
  if (slot == NULL)
  {
    // cache miss: need eviction.
    Enqueue(cache, hash, sector);

    slot = cache->front;
    ASSERT(slot != NULL);

    // fill in the cache entry.
    slot->dirty = false;
    lock_release(&buffer_cache_lock);
    buffer_cache_read(sector, slot->buffer);
    lock_acquire(&buffer_cache_lock);
  }

  // copy the data form memory into the buffer cache.
  slot->dirty = true;
  memcpy(slot->buffer, source, BLOCK_SECTOR_SIZE);

  lock_release(&buffer_cache_lock);
}

// A utility function to create a new Queue Node. The queue Node
// will store the given 'sector'
QNode *newQNode(block_sector_t sector)
{
  // Allocate memory and assign 'sector'
  QNode *temp = (QNode *)malloc(sizeof(QNode));
  temp->disk_sector = sector;

  // Initialize prev and next as NULL
  temp->prev = temp->next = NULL;

  return temp;
}

// A utility function to create an empty Queue.
// The queue can have at most 'numberOfSector' nodes
Queue *createQueue(int numberOfSector)
{
  Queue *queue = (Queue *)malloc(sizeof(Queue));

  // The queue is empty
  queue->count = 0;
  queue->front = queue->rear = NULL;

  // Number of sector that can be stored in memory
  queue->numberOfSector = numberOfSector;

  return queue;
}

// free queue
void freeQueue(Queue *queue)
{
  while (queue->count > 0)
  {
    deQueue(queue);
  }
}

// A utility function to create an empty Hash of given capacity
Hash *createHash(int capacity)
{
  // Allocate memory for hash
  Hash *hash = (Hash *)malloc(sizeof(Hash));
  hash->capacity = capacity;

  // Create an array of pointers for refering queue nodes
  hash->array = (QNode **)malloc(hash->capacity * sizeof(QNode *));

  // Initialize all hash entries as empty
  int i;
  for (i = 0; i < hash->capacity; ++i)
    hash->array[i] = NULL;

  return hash;
}

// A function to check if there is slot available in memory
int AreAllSectorFull(Queue *queue)
{
  return queue->count == queue->numberOfSector;
}

// A utility function to check if queue is empty
int isQueueEmpty(Queue *queue)
{
  return queue->rear == NULL;
}

// A utility function to delete a sector from queue
void deQueue(Queue *queue)
{
  if (isQueueEmpty(queue))
    return;

  // If this is the only node in list, then change front
  if (queue->front == queue->rear)
    queue->front = NULL;

  // Change rear and remove the previous rear
  QNode *temp = queue->rear;
  queue->rear = queue->rear->prev;

  if (queue->rear)
    queue->rear->next = NULL;

  // flush to disk
  if (temp->dirty)
  {
    buffer_cache_flush(temp);
  }

  free(temp);

  // decrement the number of full sector by 1
  queue->count--;
}

// get hash location
uint32_t hashFun(Hash *hash, block_sector_t sectorNum)
{
  uint32_t tmp = sectorNum % hash->capacity;
  uint32_t findCount = 0;

  while (findCount < hash->capacity)
  {
    if (hash->array[tmp] == NULL)
    {
      return tmp;
    }
    else if (hash->array[tmp]->disk_sector == sectorNum)
    {
      return tmp;
    }

    tmp = (tmp + 1) % hash->capacity;
    findCount++;
  }

  return -1;
}

// A function to add a sector with given 'sectorNumber' to both queue
// and hash
void Enqueue(Queue *queue, Hash *hash, unsigned sectorNumber)
{
  // If all sectors are full, remove the sector at the rear
  if (AreAllSectorFull(queue))
  {
    // remove sector from hash
    hash->array[hashFun(hash, sectorNumber)] = NULL;
    deQueue(queue);
  }

  // Create a new node with given sector number,
  // And add the new node to the front of queue
  QNode *temp = newQNode(sectorNumber);
  temp->next = queue->front;

  // If queue is empty, change both front and rear pointers
  if (isQueueEmpty(queue))
    queue->rear = queue->front = temp;
  else // Else change the front
  {
    queue->front->prev = temp;
    queue->front = temp;
  }

  // Add sector entry to hash also
  hash->array[hashFun(hash, sectorNumber)] = temp;

  // increment number of full sectors
  queue->count++;

  return temp;
}

// This function is called when a sector with given 'sectorNumber' is referenced
// from cache (or memory). There are two cases:
// 1. Sector is not there in memory, we bring it in memory and add to the front
//    of queue
// 2. Sector is there in memory, we move the sector to front of queue
void ReferenceSector(Queue *queue, Hash *hash, unsigned sectorNumber)
{
  QNode *reqSector = hash->array[hashFun(hash, sectorNumber)];

  // the sector is not in cache, bring it
  if (reqSector == NULL)
    Enqueue(queue, hash, sectorNumber);

  // sector is there and not at front, change pointer
  else if (reqSector != queue->front)
  {
    // Unlink rquested sector from its current location
    // in queue.
    reqSector->prev->next = reqSector->next;
    if (reqSector->next)
      reqSector->next->prev = reqSector->prev;

    // If the requested sector is rear, then change rear
    // as this node will be moved to front
    if (reqSector == queue->rear)
    {
      queue->rear = reqSector->prev;
      queue->rear->next = NULL;
    }

    // Put the requested sector before current front
    reqSector->next = queue->front;
    reqSector->prev = NULL;

    // Change prev of current front
    reqSector->next->prev = reqSector;

    // Change front to the requested sector
    queue->front = reqSector;
  }
}