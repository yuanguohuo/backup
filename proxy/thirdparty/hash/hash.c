#include <stdio.h>
#include <stdlib.h>

#define ngx_hash(key, c) ((unsigned int)key*31 + c)
unsigned int ngx_hash_key(const unsigned char *data, size_t len, size_t n)
{
    unsigned int i,key;
    key = 1315423911;
    for(i = 0; i < len; i++){
        key ^= ((key << 5) + data[i] + (key >> 2));
    }
    
    return key%n;
}

