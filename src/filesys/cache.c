#include <debug.h>
#include <string.h>
#include "filesys/cache.h"
#include "filesys/filesys.h"
#include "threads/synch.h"

/* Buffer cache 容量 */
#define BUFFER_CACHE_SIZE 64

/* cache条目结构 */
typedef struct buffer_cache_entry_t
{
  struct buffer_cache_entry_t *prev, *next; /* 双重指针 */
  block_sector_t disk_sector;               /* 缓存的扇区号 */
  uint8_t buffer[BLOCK_SECTOR_SIZE];        /* 用来缓存实际数据 */
  bool dirty;                               /* dirty位，用来缓存实际的数据 */
} cache_entry;

/* cache队列 */
typedef struct cache_queue
{
  unsigned count;            /* cache队列中当前存在的扇区数量 */
  unsigned numberOfSector;   /* cache队列可容纳的扇区数量 */
  cache_entry *front, *rear; /* 头尾指针 */
} cache_queue;

/* cache哈希，用于快速查找cache条目 */
typedef struct cache_map
{
  int capacity;        /* 容量 */
  cache_entry **array; /* 指向该位置对应的cache条目 */
} cache_map;

cache_entry *new_cache_entry(block_sector_t sector);

cache_queue *create_cache_queue(int numberOfSector);

cache_map *create_cache_map(int capacity);

int is_cache_full(cache_queue *queue);

int is_cache_empty(cache_queue *queue);

void de_cache_queue(cache_queue *queue);

uint32_t cache_hash(cache_map *my_cache_map, block_sector_t sector);

void add_cache_entry(cache_queue *queue, cache_map *my_cache_map, unsigned sector);

void reference_sector(cache_queue *queue, cache_map *my_cache_map, unsigned sector);

void free_cache_queue(cache_queue *queue);

cache_entry *read_ahead(block_sector_t sector);

/* cache队列 */
static cache_queue *my_cache_queue;

/* cache 哈希表 */
static cache_map *my_cache_map;

/* 预读的 window大小 */
static uint32_t read_head_window_size = 1;

/*
 * 预读基准
 * -1表示当前基准无效
 */
static uint32_t read_head_base = -1;

/* buffer cache 操作同步锁 */
static struct lock buffer_cache_lock;

/*
 * buffer cache 初始化
 */
void buffer_cache_init(void)
{
  lock_init(&buffer_cache_lock);
  my_cache_queue = create_cache_queue(BUFFER_CACHE_SIZE);
  my_cache_map = create_cache_map(BUFFER_CACHE_SIZE);
}

/**
 * 将cache中的数据写回磁盘
 * 必须带锁操作
 */
static void buffer_cache_flush(struct buffer_cache_entry_t *entry)
{
  ASSERT(lock_held_by_current_thread(&buffer_cache_lock));
  ASSERT(entry != NULL);

  /* 将dirty的扇区写回磁盘  */
  if (entry->dirty)
  {
    block_write(fs_device, entry->disk_sector, entry->buffer);
    entry->dirty = false;
  }
}

/*
 * 关闭buffer cache
 */
void buffer_cache_close(void)
{
  /* flush cache */
  lock_acquire(&buffer_cache_lock);

  size_t i;
  for (i = 0; i < my_cache_map->capacity; ++i)
  {
    if (my_cache_map->array[i]->dirty)
      buffer_cache_flush(my_cache_map->array[i]);
  }

  /* 释放队列*/
  free_cache_queue(my_cache_queue);
  free(my_cache_map->array);

  lock_release(&buffer_cache_lock);
}

/**
 * 在缓存中查找cache条目
 * 哈希查找
 * 如果没找到，则返回NULL
 */
static struct buffer_cache_entry_t *buffer_cache_lookup(block_sector_t sector)
{
  /* 没找到 */
  if (cache_hash(my_cache_map, sector) == -1)
  {
    return (NULL);
  }

  return (my_cache_map->array[cache_hash(my_cache_map, sector)]);
}

/*
 * 读数据
 */
void buffer_cache_read(block_sector_t sector, void *target)
{
  lock_acquire(&buffer_cache_lock);
  cache_entry *slot = read_ahead(sector);

  /* reger cache，更新其优先级 */
  reference_sector(my_cache_queue, my_cache_map, sector);

  /* 将数据复制到target  */
  memcpy(target, slot->buffer, BLOCK_SECTOR_SIZE);

  /*同步预读 */
  if ( read_head_base == -1 ||slot->disk_sector > read_head_base + read_head_window_size || slot->disk_sector < read_head_base)
  {
    read_ahead(sector + 1);
    read_head_window_size = 1;

    read_head_base = sector;
  }
  else
  {
    /* 异步预读 */
    size_t i = 0;
    for (i = read_head_base + read_head_window_size + 1; i <= read_head_base + read_head_window_size + 3; i++)
    {
      read_ahead(i);
    }

    /* 预读window以3为步长逐渐增长 */
    read_head_window_size = read_head_window_size + 3;
  }

  lock_release(&buffer_cache_lock);
}

/*
 * 预读
 */
cache_entry *read_ahead(block_sector_t sector)
{
  struct buffer_cache_entry_t *slot = buffer_cache_lookup(sector);
  if (slot == NULL)
  {
    /* 扇区不在cache中，从磁盘中读取 */
    add_cache_entry(my_cache_queue, my_cache_map, sector);
    slot = my_cache_queue->front;
  }

  return (slot);
}

/*
 * 写数据到磁盘
 */
void buffer_cache_write(block_sector_t sector, const void *source)
{
  lock_acquire(&buffer_cache_lock);

  struct buffer_cache_entry_t *slot = buffer_cache_lookup(sector);
  if (slot == NULL)
  {
    /* 扇区不在cache中，从磁盘中读取 */
    add_cache_entry(my_cache_queue, my_cache_map, sector);

    slot = my_cache_queue->front;
    ASSERT(slot != NULL);

    slot->dirty = false;
    lock_release(&buffer_cache_lock);
    buffer_cache_read(sector, slot->buffer);
    lock_acquire(&buffer_cache_lock);
  }

  /* 将数据复制到缓存中  */
  slot->dirty = true;
  memcpy(slot->buffer, source, BLOCK_SECTOR_SIZE);

  lock_release(&buffer_cache_lock);
}

/*
 * 创建一个新的cache条目
 * 存储特定扇区的数据
 */
cache_entry *new_cache_entry(block_sector_t sector)
{
  /* 分配内存 */
  cache_entry *temp = (cache_entry *)malloc(sizeof(cache_entry));
  temp->disk_sector = sector;

  temp->prev = temp->next = NULL;

  return (temp);
}

/*
 * 创建空cache队列
 */
cache_queue *create_cache_queue(int numberOfSector)
{
  cache_queue *queue = (cache_queue *)malloc(sizeof(cache_queue));

  /* 队列为空 */
  queue->count = 0;
  queue->front = queue->rear = NULL;

  /* 队列容量 */
  queue->numberOfSector = numberOfSector;

  return (queue);
}

/*
 * 释放队列
 */
void free_cache_queue(cache_queue *queue)
{
  while (queue->count > 0)
  {
    de_cache_queue(queue);
  }
}

/*
 * 创建一个新的cache哈希表
 */
cache_map *create_cache_map(int capacity)
{
  cache_map *my_cache_map = (cache_map *)malloc(sizeof(cache_map));
  my_cache_map->capacity = capacity;

  /* 指向cache条目 */
  my_cache_map->array = (cache_entry **)malloc(my_cache_map->capacity * sizeof(cache_entry *));

  /* 初始化为空 */
  int i;
  for (i = 0; i < my_cache_map->capacity; ++i)
    my_cache_map->array[i] = NULL;

  return (my_cache_map);
}

/*
 * cache队列是否已满
 */
int is_cache_full(cache_queue *queue)
{
  return (queue->count == queue->numberOfSector);
}

/*
 * cache队列是否为空
 */
int is_cache_empty(cache_queue *queue)
{
  return (queue->rear == NULL);
}

/*
 * 从cache队列中删除一个cache条目
 */
void de_cache_queue(cache_queue *queue)
{
  if (is_cache_empty(queue))
    return;

  /* 如果当前队列仅仅一个元素，设置队头为NULL */
  if (queue->front == queue->rear)
    queue->front = NULL;

  cache_entry *temp = queue->rear;
  queue->rear = queue->rear->prev;

  if (queue->rear)
    queue->rear->next = NULL;

  /* 删除cache前将数据写会磁盘 */
  if (temp->dirty)
  {
    buffer_cache_flush(temp);
  }

  free(temp);

  /* 更新cache队列长度 */
  queue->count--;
}

/*
 * cache哈希函数，使用线性探测法解决冲突
 */
uint32_t cache_hash(cache_map *my_cache_map, block_sector_t sector)
{
  uint32_t tmp = sector % my_cache_map->capacity;
  uint32_t findCount = 0;

  while (findCount < my_cache_map->capacity)
  {
    if (my_cache_map->array[tmp] == NULL)
    {
      return (tmp);
    }
    else if (my_cache_map->array[tmp]->disk_sector == sector)
    {
      return (tmp);
    }

    tmp = (tmp + 1) % my_cache_map->capacity;
    findCount++;
  }

  return (-1);
}

/*
 * 添加cache条目，入队
 */
void add_cache_entry(cache_queue *queue, cache_map *my_cache_map, unsigned sector)
{
  /* 如果队列满，执行出队操作 */
  if (is_cache_full(queue))
  {
    /* 删除队尾cache条目 */
    my_cache_map->array[cache_hash(my_cache_map, sector)] = NULL;
    de_cache_queue(queue);
  }

  /*
	 * 新建一个cache条目
	 * 添加到队头
	 */
  cache_entry *temp = new_cache_entry(sector);
  temp->next = queue->front;

  /* 如果cache队列为空，队列头尾都是该节点 */
  if (is_cache_empty(queue))
    queue->rear = queue->front = temp;
  else
  {
    queue->front->prev = temp;
    queue->front = temp;
  }

  /* 添加新条目到cache哈希表 */
  my_cache_map->array[cache_hash(my_cache_map, sector)] = temp;

  temp->dirty = false;
  block_read(fs_device, sector, temp->buffer);

  queue->count++;

  return (temp);
}

/*
 * refer 缓存
 * 更新其优先级
 * 两种情况：
 *    1.存在cache中，放到队投
 *    2.不在cache中，添加条目
 */
void reference_sector(cache_queue *queue, cache_map *my_cache_map, unsigned sector)
{
  cache_entry *req_sector = my_cache_map->array[cache_hash(my_cache_map, sector)];

  /* 如果扇区不在cache中，添加cache条目 */
  if (req_sector == NULL)
    add_cache_entry(queue, my_cache_map, sector);

  else if (req_sector != queue->front)
  {
    req_sector->prev->next = req_sector->next;
    if (req_sector->next)
      req_sector->next->prev = req_sector->prev;

    /* 如果reger条目在队尾 */
    if (req_sector == queue->rear)
    {
      queue->rear = req_sector->prev;
      queue->rear->next = NULL;
    }

    /* 放到队头 */
    req_sector->next = queue->front;
    req_sector->prev = NULL;

    req_sector->next->prev = req_sector;

    queue->front = req_sector;
  }
}