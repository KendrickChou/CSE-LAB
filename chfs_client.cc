// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <list>

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))


chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
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
    printf("isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
        extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("dir: %lld is not a dir\n", inum);
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMLINK){
        printf("issymlink: %lld is a symlink\n", inum);
        return true;
    } 
    printf("issymlink: %lld is not a symlink\n", inum);
    return false;
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
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    printf("setattr: %llu\n", ino);
    std::string data;
    ec->get(ino, data);
    data.resize(size, '\0');
    ec->put(ino, data);

    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("create: %s\n", name);
    bool existed = false;
    lookup(parent, name, existed, ino_out);

    if(existed){
        printf("file %s existed\n", name);
        r = EXIST;
        return r;
    }

    ec->create(extent_protocol::T_FILE, ino_out);


    std::string parent_entries;
    ec->get(parent, parent_entries);
    parent_entries += name;
    parent_entries += ("/" + std::to_string(ino_out) + "/");
    ec->put(parent, parent_entries);

    return r;
}


int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("mkdir: %s\n", name);
    bool existed = false;
    lookup(parent, name, existed, ino_out);

    if(existed){
        printf("dir %s existed\n", name);
        r = EXIST;
        return r;
    }

    ec->create(extent_protocol::T_DIR, ino_out);

    std::string parent_entries;
    ec->get(parent, parent_entries);
    parent_entries += name;
    parent_entries += '/' + std::to_string(ino_out) + "/";
    ec->put(parent, parent_entries);

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
    printf("lookup %s\n", name);
    if(!isdir(parent)){
        printf("lookup ERROR: %llu is not a dir\n", parent);
        found = false;
        return r;
    }
    std::string dir_entries;
    ec->get(parent, dir_entries);

    int iter = 0;
    while (iter < dir_entries.size()) {
        dirent entry = parse_a_dirent(dir_entries, iter);
        if (!entry.name.compare(name)) {
            ino_out = entry.inum;
            found = true;
            return r;
        }
    }

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

    printf("readdir %llu\n", dir);
    if(!isdir(dir)){
        printf("lookup ERROR: %llu is not a dir\n", dir);
        return IOERR;
    }
    std::string dir_entries;
    ec->get(dir, dir_entries);

    int iter = 0;
    while (iter < dir_entries.size()) {
        dirent entry = parse_a_dirent(dir_entries, iter);
        list.push_back(entry);
    }

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
    printf("read %llu\n", ino);
    if(!isfile(ino)){
        return IOERR;
    }

    std::string file_data;
    r = ec->get(ino, file_data);

    if (r != extent_protocol::OK) {
        return r;
    }

    data = file_data.substr(off, size);

    return r;
}

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

    if (off < 0) {
        printf("write Error: %llu offset < 0\n", ino);
        return r;
    }

    std::string origin_data;
    ec->get(ino, origin_data);

    size_t origin_size = origin_data.size();
    printf("origin size: %d\n", origin_data.size());
    bytes_written = size;

    if(off + size > origin_size){
        origin_data.resize(off+size, '\0');
    }

    memcpy((void *)(origin_data.c_str() + off), data, size);

    ec->put(ino, origin_data);

    printf("write %llu, written %lu bytes\n", ino, bytes_written);

    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    inum ino = 0;
    bool found = false;
    lookup(parent, name, found, ino);

    if(!found){
        return NOENT;
    }
    
    if ((r = ec->remove(ino))  != extent_protocol::OK){
        return r;
    }

    std::string new_entries = "";
    std::list<dirent> dirents;
    
    readdir(parent, dirents);
    for(auto iter = dirents.begin(); iter != dirents.end(); ++iter){
        if(iter->name != std::string(name)){
            new_entries += iter->name + "/" + std::to_string(iter->inum) + "/";
        }
    }
    r = ec->put(parent, new_entries);

    return r;
}

chfs_client::dirent
chfs_client::parse_a_dirent(const std::string &entries, int &iter) {
    dirent entry;
    while(entries[iter] != '/'){
        entry.name += entries[iter];
        iter++;
    }
    std::string num;
    while(++iter < entries.size() && entries[iter] != '/'){
        num += entries[iter];
    }
    entry.inum = std::stoull(num);
    ++iter;
    return entry;
}

int
chfs_client::symlink(inum parent, const char *link, const char *name, inum &ino_out)
{
    int r = OK;
    bool existed = false;
    lookup(parent, name, existed, ino_out);

    if(existed){
        printf("file %s existed\n", name);
        r = EXIST;
        return r;
    }

    r = ec->create(extent_protocol::T_SYMLINK, ino_out);
    if (r != extent_protocol::OK){
        return r;
    }
    std::string link_str;
    std::string name_str;
    link_str.assign(link, strlen(link));
    name_str.assign(name, strlen(name));

    r = ec->put(ino_out, link_str);
    if (r != extent_protocol::OK){
        return r;
    }

    std::string parent_entries;
    r = ec->get(parent, parent_entries);
    if (r != extent_protocol::OK){
        return r;
    }

    parent_entries += name_str + '/' + std::to_string(ino_out) + '/';
    r = ec->put(parent, parent_entries);

    printf("create symlink successfully\n");
    return r;
}

int
chfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;

    if(!issymlink(ino)){
        printf("readlink : %d is not a symlink\n", ino);
        return NOENT;
    }

    ec->get(ino, data);
    std::cout << data;

    return r;
}

