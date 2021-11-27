#include "inode_manager.h"
#include "string.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  //how to protect next block?
  if (id < BLOCK_NUM) {
    memcpy((void *) buf, (void *) blocks[id], BLOCK_SIZE);
  } else {
    printf("read_block out of blocks range!\n");
  }
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if (id < BLOCK_NUM) {
    memcpy((void *) blocks[id], (void *) buf, BLOCK_SIZE);
  } else {
    printf("write_block out of blocks range!\n");
  }
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  char bitmap_buf[BLOCK_SIZE];
  int free_block_pos = 0;
  for (int i = 0; i < BBN; ++i) {
    read_block(i, bitmap_buf);

    for (int j = 0; j < BLOCK_SIZE; ++j) {
      char eight_bits = bitmap_buf[j];
      if(eight_bits != 0xff){
        for (int k = 0; k < 8; ++k){
          if(!((eight_bits >> (0x7 - k)) & 0x1)){
            //get & set it
            free_block_pos = i * BPB + j * 8 + k + DBB;
            bitmap_buf[j] |= 0x1 << (0x7 - k);
            write_block(i, bitmap_buf);
            break;
          }
        }
      }
      if (free_block_pos) break;
    }

    if (free_block_pos) {
        return free_block_pos;
    }

    if (i == BBN - 1){
      printf("no available blocks!\n");
      return 0;
    }
  }
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  
  char bitmap_buf[BLOCK_SIZE];
  read_block(BBLOCK(id), bitmap_buf);

  int bit_pos = (id - DBB) % BPB;

  bitmap_buf[bit_pos >> 3] &= ~(0x1 << (0x7 - (bit_pos % 0x8)));
  write_block(BBLOCK(id), bitmap_buf);

  char zero_block[BLOCK_SIZE] = {0};
  write_block(id, zero_block);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();

  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */

  inode *new_ino = NULL;

  char buf[BLOCK_SIZE];
  uint32_t iter = 1;
  for(; iter <= INODE_NUM; ++iter){
    bm->read_block(IBLOCK(iter), buf);
    new_ino = (struct inode*)buf + iter%IPB;
    if (new_ino->type == 0) break;
  }

  if(iter > INODE_NUM) {
    printf("All inode is in use.\n");
    return 0;
  }

  new_ino->type = type;
  new_ino->size = 0;
  new_ino->ctime = new_ino->atime = new_ino->mtime = time(nullptr);

  put_inode(iter, new_ino);

  return iter;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  // if (inum == 1) {
  //   printf("Cannot free root inode.");
  //   return;
  // }

  inode *ino = get_inode(inum);

  if(ino == NULL) return;

  //Attension: if IPB changed, it goes WRONG!!!!
  memset(ino, 0, sizeof(inode));
  put_inode(inum, ino);
  free(ino);
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) ((a)>(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */

  inode *file_ino = get_inode(inum);

  if (file_ino == NULL) return;

  *size = file_ino->size;

  int num_blocks = (*size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  *buf_out = (char *)malloc(num_blocks * BLOCK_SIZE);
  memset(*buf_out, 0, num_blocks * BLOCK_SIZE);

  if (num_blocks == 0) {
    return;
  } else if ((long unsigned int)num_blocks > MAXFILE) {
    printf("Read file: file is too large!\n");
    return;
  }

  for (int i = 0; i < MIN(num_blocks, NDIRECT); ++i) {
    bm->read_block(file_ino->blocks[i], (*buf_out) + i * BLOCK_SIZE);
  }

  uint indirect_blocks[NINDIRECT];
  if (num_blocks > NDIRECT) {
    bm->read_block(file_ino->blocks[NDIRECT], (char *)indirect_blocks);
    for(int i = 0; i < num_blocks - NDIRECT; ++i){
        bm->read_block(indirect_blocks[i], (*buf_out) + (i + NDIRECT) * BLOCK_SIZE);
    }
  }

  file_ino->atime = time(nullptr);
  put_inode(inum, file_ino);
  //maybe overflow
  free(file_ino);
  return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  inode *file_ino = get_inode(inum);

  if (file_ino == NULL) return;

  int num_blocks = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int existed_blocks = (file_ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  if ((long unsigned int) num_blocks > MAXFILE) {
    printf("Write file: file is too large!\n");
    return;
  }

  char aligned_buf[num_blocks * BLOCK_SIZE] = {0};
  memcpy(aligned_buf, buf, size);

  if (num_blocks < existed_blocks) {
    for (int i = num_blocks; i < MIN(existed_blocks, NDIRECT); ++i){
      bm->free_block(file_ino->blocks[i]);
    }
    if (existed_blocks > NDIRECT) {
      uint indirect_blocks[NINDIRECT];
      bm->read_block(file_ino->blocks[NDIRECT], (char *)indirect_blocks);
      for(int i = MAX(0, num_blocks - NDIRECT); i < existed_blocks - NDIRECT; ++i){
        bm->free_block(indirect_blocks[i]);
      }
      if (num_blocks <= NDIRECT){
        bm->free_block(file_ino->blocks[NDIRECT]);
      }
    }
  } else if (num_blocks > existed_blocks)
  {
    for(int i = existed_blocks; i < MIN(num_blocks, NDIRECT); ++i){
      file_ino->blocks[i] = bm->alloc_block();
    }
    if(num_blocks > NDIRECT) {
      uint indirect_blocks[NINDIRECT];

      if (existed_blocks > NDIRECT){
        bm->read_block(file_ino->blocks[NDIRECT], (char *)indirect_blocks);
      } else{
        file_ino->blocks[NDIRECT] = bm->alloc_block();
      }

      for(int i = MAX(0, existed_blocks-NDIRECT); i < num_blocks-NDIRECT; ++i){
        indirect_blocks[i] = bm->alloc_block();
      }
      bm->write_block(file_ino->blocks[NDIRECT], (char *)indirect_blocks);
    }
  }
  

  for (int i = 0; i < MIN(num_blocks, NDIRECT); ++i){
    bm->write_block(file_ino->blocks[i], aligned_buf + i * BLOCK_SIZE);
  }

  if(num_blocks > NDIRECT){
    uint indirect_blocks[NINDIRECT];
    bm->read_block(file_ino->blocks[NDIRECT], (char *)indirect_blocks);
    for(int i = 0; i < num_blocks-NDIRECT; ++i){
      bm->write_block(indirect_blocks[i], aligned_buf + (i + NDIRECT) * BLOCK_SIZE);
    }
  }

  //remember to update wtime
  file_ino->size = size;
  file_ino->ctime = file_ino->mtime = time(nullptr);
  put_inode(inum, file_ino);
  free(file_ino);
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode *ino = NULL;
  ino = get_inode(inum);

  if (ino == NULL) {
    printf("Getattr Error: inode does not exist!\n");
    return;
  }

  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.size = ino->size;
  a.type = ino->type;

  free(ino);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */

  inode *ino = get_inode(inum);

  if (ino == NULL) {
    printf("remove file error.\n");
    return;
  }

  int block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;

  for (int i = 0; i < MIN(block_num, NDIRECT); ++i){
    bm->free_block(ino->blocks[i]);
  }

  if (block_num > NDIRECT) {
    char indirect_buf[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], indirect_buf);
    uint *indirect_blocks = (uint *)indirect_buf;
    for (int i = 0; i < block_num - NDIRECT; ++i){
      bm->free_block(indirect_blocks[i]);
    }

    bm->free_block(ino->blocks[NDIRECT]);
  }

  free_inode(inum);
  free(ino);
  return;
}
