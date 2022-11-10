#include "inode_manager.h"

// disk layer -----------------------------------------
using namespace std;

disk::disk()
{
  bzero(blocks, sizeof(blocks));
   // void bzero(void *s,int n) 作用:bzero函数的作用是将s指针指向的地址的前n个字节清零
}

void disk::read_block(blockid_t id, char *buf)
{
  // 从block读内容到缓存buf
  memcpy(buf, blocks[id], BLOCK_SIZE);
  // void *memcpy(void *destin, void *source, unsigned n)；函数的功能是从源内存地址的起始位置开始拷贝若干个字节到目标内存地址中，即从源source中拷贝n个字节到目标destin中
}

void disk::write_block(blockid_t id, const char *buf)
{
  // 将buf内容写入block
  memcpy(blocks[id], buf, BLOCK_SIZE);
}
void
disk::snapshot_blocks(char *buf)
{
  printf("进入disk层\n");
  // for(int i = 0; i < BLOCK_NUM; i++)
  // {
  //   memcpy(buf,blocks[i],BLOCK_SIZE);
  // }
  // memcpy(buf,blocks,DISK_SIZE);
   // 打开checkpoint文件
    FILE * checkfile = fopen("log/checkpoint.bin", "wb");
    char *test = (char *)blocks;
    fwrite(blocks,1,DISK_SIZE,checkfile);
    // fwrite(blocks,1,DISK_SIZE,checkfile);
    // printf("写入的BLOCKS的内容:%S\n",blocks);
    fclose(checkfile);
  printf("disk层的copy完成\n");
}

bool
disk::restore_blocks()
{
  FILE * checkfile = fopen("log/checkpoint.bin", "r");
  if(!checkfile)
  {
    printf("之前没有触发checkpoint\n");
    fclose(checkfile);
    return false;
  }
  fread(blocks,1,DISK_SIZE,checkfile); 
  fclose(checkfile);
  printf("disk层的恢复完成\n");
  return true;
}


// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  for(int i = IBLOCK(INODE_NUM, sb.nblocks) + 1; i < BLOCK_NUM; i++)
    if(!using_blocks[i]){
      using_blocks[i] = true;
      return i;
    }
  printf("\tim: error! alloc_block failed!\n");
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  using_blocks[id] = 0;
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

void
block_manager::snapshot_blocks(char *buf)
{
  d->snapshot_blocks(buf);
  printf("bm层的copy完成\n");
}

bool
block_manager::restore_blocks()
{
  return d->restore_blocks();
}


// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
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
  int inum = 0;

  for(int i = 0; i < INODE_NUM; i++){
    inum = (inum + 1) % INODE_NUM;//in case of out of range
    inode_t *ino = get_inode(inum);
    if(!ino){
      ino = (inode_t *)malloc(sizeof(inode_t));
      bzero(ino, sizeof(inode_t));
      ino->type = type;
      ino->atime = (unsigned int)time(NULL);
      ino->mtime = (unsigned int)time(NULL);
      ino->ctime = (unsigned int)time(NULL);
      put_inode(inum, ino);
      free(ino);
      break;
    }
    free(ino);
  }

  assert(inum);
  return inum;
}

void inode_manager::free_inode(uint32_t inum)
{
  inode_t *ino = get_inode(inum);
  if (!ino || !(ino->type))
  {
        exit(0);
  }
  if(ino)
  {
    //还没有free过
    ino->type = 0;

    //write back to disk
    put_inode(inum,ino);
    free(ino);
  }
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  char buf[BLOCK_SIZE];
  struct inode *ino_tmp;

  if(inum >= INODE_NUM ||inum < 0)
  {
    printf("\twhen get_inode im: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  

  ino_tmp = (struct inode*)buf + inum%IPB;

  if(!ino_tmp->type)
  {
    // printf("\twhen get_inode im: inode not exist\n");
    return NULL;
  }

  
  ino = (struct inode*)malloc(sizeof(struct inode));
  // printf("\twhen get_inode TEST\n");
  *ino = *ino_tmp;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;
  if (ino == NULL)
    return;
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;//这个取模应该是为一个block多个inode准备的
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

//my functions
blockid_t
inode_manager::get_nth_blockid(inode_t *ino, uint32_t n)
{
  blockid_t ans;
  assert(ino);

  if(n < NDIRECT)//direct
    ans = ino->blocks[n];

  else if(n < MAXFILE)
  {
    if(!ino->blocks[NDIRECT])
    {
      printf("nth_blockid none NINDIRECT!\n");//return here?
    }

    char tmp[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT],tmp);

    ans = ((blockid_t *)tmp)[n-NDIRECT];
  }
  else//out of range
  {
    printf("nth_blockid out of range\n");
    exit(0);
  }
  return ans;
}

void 
inode_manager::alloc_nth_block(inode_t *ino, uint32_t n)
{
  assert(ino);
  if(n < NDIRECT)
    ino->blocks[n] = bm->alloc_block();
  else if(n < MAXFILE)
  {
    if(!ino->blocks[NDIRECT])
    {
      ino->blocks[NDIRECT] = bm->alloc_block();
    }

    char tmp[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT],tmp);

    ((blockid_t *)tmp)[n-NDIRECT] = bm->alloc_block();

    bm->write_block(ino->blocks[NDIRECT],tmp);
  }
  else//out of range
  {
    printf("nth_blockid out of range\n");
    exit(0);
  }
}


/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  int block_num = 0;
  int remain_size = 0;
  char buf[BLOCK_SIZE];
  int i = 0;

  printf("\tim: read_file %d\n", inum);
  inode_t *ino = get_inode(inum);
  if(ino){
    *size = ino->size;
    assert((unsigned int)*size <= MAXFILE * BLOCK_SIZE);
    *buf_out = (char *)malloc(*size);

    block_num = *size / BLOCK_SIZE;
    remain_size = *size % BLOCK_SIZE;

    for(; i < block_num; i++){
      bm->read_block(get_nth_blockid(ino, i), buf);
      memcpy(*buf_out + i*BLOCK_SIZE, buf, BLOCK_SIZE);
    }
    if(remain_size){
      bm->read_block(get_nth_blockid(ino, i), buf);
      memcpy(*buf_out + i*BLOCK_SIZE, buf, remain_size);
      // printf("\tim: read_file %s %d\n", *buf_out, *size);
    }
    free(ino);
  }
  return;
}



/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  int block_num = 0;
  int remain_size = 0;
  int old_blocks, new_blocks;
  char temp[BLOCK_SIZE];
  int i = 0;

  assert(size >= 0 && (uint32_t)size <= MAXFILE * BLOCK_SIZE);
  printf("\tim: write_file %d of size %d\n", inum, size);
  inode_t *ino = get_inode(inum);
  if(ino){
    assert((unsigned int)size <= MAXFILE * BLOCK_SIZE);
    assert(ino->size <= MAXFILE * BLOCK_SIZE);

    old_blocks = ino->size == 0? 0 : (ino->size - 1)/BLOCK_SIZE + 1;
    new_blocks = size == 0? 0 : (size - 1)/BLOCK_SIZE + 1;
    if(old_blocks < new_blocks)
      for(int j = old_blocks; j < new_blocks; j++)
        alloc_nth_block(ino, j);
    else if(old_blocks > new_blocks)
      for(int j = new_blocks; j < old_blocks; j++)
        bm->free_block(get_nth_blockid(ino, j));

    block_num = size / BLOCK_SIZE;
    remain_size = size % BLOCK_SIZE;

    for(; i < block_num; i++)
      bm->write_block(get_nth_blockid(ino, i), buf + i*BLOCK_SIZE);
    if(remain_size){
      memcpy(temp, buf + i*BLOCK_SIZE, remain_size);
      bm->write_block(get_nth_blockid(ino, i), temp);
    }

    ino->size = size;
    ino->atime = time(NULL);
    ino->mtime = time(NULL);
    ino->ctime = time(NULL);
    put_inode(inum, ino);
    free(ino);
  }
}


void inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  inode_t *inode = get_inode(inum);
  if (!inode)
    return;
  a.type = inode->type;
  a.atime = inode->atime;
  a.mtime = inode->mtime;
  a.ctime = inode->ctime;
  a.size = inode->size;
  free(inode);
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  // printf("\tim: remove_file %d\n", inum);
  inode_t *ino = get_inode(inum);
  assert(ino);

  int block_num = ino->size == 0? 0 : (ino->size - 1)/BLOCK_SIZE + 1;
  for(int i = 0; i < block_num; i++)
    bm->free_block(get_nth_blockid(ino, i));
  if(block_num > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);
  bzero(ino, sizeof(inode_t));

  free_inode(inum);
  free(ino);
  return;
}

void
inode_manager::snapshot_blocks(char *buf)
{
  bm->snapshot_blocks(buf);
  printf("im层的copy完成\n");
}

bool
inode_manager::restore_blocks()
{
  return bm->restore_blocks();
}

bool 
inode_manager::save_inodes()
{
  // 打开checkpoint文件
  FILE * checkfile = fopen("log/checkpoint.bin", "wb");
    
  for(int i = 0; i < INODE_NUM; i++)
  {
    inode_t *ino = get_inode(i);
    if(!ino)
    continue;

    char *cbuf = NULL;
    int size = 0;
    read_file(i, &cbuf, &size);
    
    fixed_struct set;
    set.assign(i,ino->type,(unsigned long long)size);
    fwrite(&set,sizeof(set),1,checkfile);
    
    fwrite(cbuf,1,size,checkfile);
  }
  fclose(checkfile);
   printf("im copy完有内容的inode\n");
}

bool
inode_manager::restore_inodes()
{
  // 打开checkpoint文件
    FILE* checkfile = fopen("log/checkpoint.bin", "rb");
    if(checkfile == NULL){
        printf("checkpoint未被触发 \n");
        return 0;
    }
    while(!feof(checkfile)){
        std::string content = "xxx";
        fixed_struct set;
        fread(&set, sizeof(set), 1, checkfile);
        std::string cbuf;
        cbuf.resize(set.buf_size);
        fread((char*)cbuf.c_str(), set.buf_size, 1, checkfile);
        
       //重新分配+put
      //  这里为啥不用alloc:不一定会返回我想要的那个inum
       int inum = set.inode;
       inode_t *ino = get_inode(inum);
          if(!ino){
          ino = (inode_t *)malloc(sizeof(inode_t));
          bzero(ino, sizeof(inode_t));
          ino->type = set.type;
          // to see if it need to be recorded
          ino->atime = (unsigned int)time(NULL);
          ino->mtime = (unsigned int)time(NULL);
          ino->ctime = (unsigned int)time(NULL);
          put_inode(inum, ino);
          free(ino);
          }
          else{
            printf("inode居然之前存在吗 不可思议\n");
            bzero(ino, sizeof(inode_t));
            ino->type = set.type;
            // TODO: to see if it need to be recorded
            ino->atime = (unsigned int)time(NULL);
            ino->mtime = (unsigned int)time(NULL);
            ino->ctime = (unsigned int)time(NULL);
            put_inode(inum, ino);
            free(ino);
            // return 0;
          }
      //put inode? write file?
      write_file(inum,cbuf.c_str(),set.buf_size);
    
    }
    fclose(checkfile);
    printf("我恢复完所有的之前写过的文件啦\n");
    return true;

}