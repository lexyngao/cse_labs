// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */

chfs_client::chfs_client()
{
    ec = new extent_client();

}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */
int chfs_client::readlink(inum ino, std::string &data)
{
	int r = OK;
	std::string buf;
	if(ec->get(ino, buf)!=extent_protocol::OK)
        return IOERR;
	
	data = buf;
	return r;
}

// bool chfs_client::issymlink(inum inum)
// {
//     extent_protocol::attr a;

//     if(ec->getattr(inum,a)!=extent_protocol::OK)
//         return 0;
    
//     if(a.type == extent_protocol::T_SYMLINK)
//         return 1;
//     return 0;
// }

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    // return ! isfile(inum);
    extent_protocol::attr a;

    if(ec->getattr(inum,a)!=extent_protocol::OK)
        return 0;
    
    if(a.type == extent_protocol::T_DIR)
        return 1;
    return 0;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    std::string buf;
    if(ec->get(ino,buf)!= extent_protocol::OK)
    {
        r = IOERR;
        return r;
    }

    //直接resize
    buf.resize(size);

    if (ec->put(ino, buf) != extent_protocol::OK) {
        r = IOERR;
    }

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    //check if already existed
    bool existed = false;
    inum tmp;
    lookup(parent,name,existed,tmp);
    if(existed)
        return EXIST;

    //choose a inum
    ec->create(extent_protocol::T_FILE, ino_out);

    //modify parent
    std::string tmp_buf;
    ec->get(parent,tmp_buf);
    tmp_buf.append((std::string)name+":"+filename(ino_out)+"/");
    ec->put(parent,tmp_buf);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    //exactly like "create"
    //check if already existed
    bool existed = false;
    inum tmp;
    lookup(parent,name,existed,tmp);
    if(existed)
        return EXIST;

    //choose a inum
    ec->create(extent_protocol::T_DIR, ino_out);

    //modify parent
    std::string tmp_buf;
    ec->get(parent,tmp_buf);
    tmp_buf.append((std::string)name+":"+filename(ino_out)+"/");
    ec->put(parent,tmp_buf);

    return r;
}

int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */

    std::list<dirent> list;

    readdir(parent,list);

    if(list.empty())
    {
        found = 0;
        return r;
    }


    for (std::list<dirent>::iterator it = list.begin(); it != list.end(); it++) {
		if (it->name.compare(name) == 0) { /* find */
			found = true;
			ino_out = it->inum;
			return r;
		}
	};
	found = false;

    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    
    /* format of dir content: "name:inum/name:inum/name:inum/" */
	extent_protocol::attr a;

    if(ec->getattr(dir,a)!= extent_protocol::OK)
    {
        r = IOERR;
        return r;
    }

    int name_start = 0;
    std::string buf;
    if(ec->get(dir,buf)!= extent_protocol::OK)
    {
        r = IOERR;
        return r;
    }

	int name_end = buf.find(':');

	while (name_end != std::string::npos) {
		/* get name & inum */
		std::string name = buf.substr(name_start, name_end - name_start);

		int inum_start = name_end + 1;
		int inum_end = buf.find('/', inum_start);

		std::string inum = buf.substr(inum_start, inum_end - inum_start);
		
		struct dirent entry;
		entry.name = name;
		entry.inum = n2i(inum);
		
		list.push_back(entry);
		
		name_start = inum_end + 1;
		name_end = buf.find(':', name_start);
	};

    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    if(ec->get(ino,buf)!=extent_protocol::OK)
    {
        r = IOERR;
        return r;
    }

    //全部溢出
    if((uint32_t)off >= buf.size())
    {
        data = "";
        return r;
    }
        
    
    //part overflow
    if((uint32_t)off + size > buf.size())
    {
        data = buf.substr(off);
        return r;
    }

    //normal
    data = buf.substr(off,size);

    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    if(ec->get(ino,buf)!=extent_protocol::OK)
    {
        r = IOERR;
        return r;
    }

    
    if (off + size > buf.size()) //TODO：是否需要bytes_written变为off+size-buf.size()
        buf.resize(off + size,'\0'); /* 改变 buf 大小 */

    for(int i = 0; i < size ; i++)
        buf[off+i] = data[i];
             
	bytes_written = size;
	if(ec->put(ino, buf)==extent_protocol::OK)
		return r;
    else
        return IOERR;

  
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */

    //check if already existed
    bool existed = false;
    inum tmp;
    lookup(parent,name,existed,tmp);
    if(!existed)
        return NOENT;

    //remove a inum
    if(ec->remove(tmp)!=extent_protocol::OK)
        return IOERR;

    //modify parent
    std::string buf;
    ec->get(parent,buf);
    
	int erase_start = buf.find(name);
	int erase_after = buf.find('/', erase_start);
	buf.erase(erase_start, erase_after - erase_start + 1);
	if (ec->put(parent, buf) != extent_protocol::OK) 
       return IOERR;
        
    return r;
}

int chfs_client::symlink(inum parent, const char* name, const char* link, inum &ino_out)
{
    int r = OK;
    //check if already existed
    bool existed = false;
    inum tmp;
    lookup(parent,name,existed,tmp);
    if(existed)
        return EXIST;

    //choose a inum
    if(ec->create(extent_protocol::T_SYMLINK, ino_out)!=extent_protocol::OK)
        return IOERR;
    if(ec->put(ino_out,(std::string)link)!=extent_protocol::OK)
    {
        return IOERR;
    }

    //modify parent
    std::string tmp_buf;
    if(ec->get(parent,tmp_buf)!=extent_protocol::OK)
        return IOERR;
    tmp_buf.append((std::string)name+":"+filename(ino_out)+"/");
    if(ec->put(parent,tmp_buf)!=extent_protocol::OK)
        return IOERR;
    return r;
}
