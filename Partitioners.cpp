//  Copyright 2019 ThreatMetrix
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
//  Partitioners.cpp
//  Implements the various partitioners used by Cassandra.

#include "Partitioners.hpp"

#include <cstring>
#include <limits>

extern "C"
{
#include <openssl/md5.h>
}

class RandomPartitioner : public Partitioner
{
public:
    virtual void assign_token(CassandraParser::Token & token, const char * key, size_t key_length) const
    {
        CassandraParser::Token checksum;
        MD5_CTX md5;
        MD5_Init(&md5);
        MD5_Update(&md5, key, key_length);
        MD5_Final(checksum, &md5);

        if (checksum[0] < 0x80)
        {
            std::memcpy(token, checksum, sizeof(CassandraParser::Token));
        }
        else
        {
            // Doing a absolute value of MD5 treated as twos compliment
            int i = 15;
            while(checksum[i] == 0) // Mustn't match first byte, due to condition above
            {
                token[i] = 0;
                i--;
            }

            token[i] = 0xFF - checksum[i] + 1;
            i--;

            for (; i >= 0; i--)
            {
                token[i] = 0xFF - checksum[i];
            }
        }
    }

    virtual int compare_token(const CassandraParser::Token & tokenA, const std::string & keyA, const CassandraParser::Token & tokenB, const std::string & keyB) const
    {
        const int result = std::memcmp(tokenA, tokenB, 16);
        if (result != 0)
            return result;

        const int content_cmp = std::memcmp(keyA.data(), keyB.data(), std::min(keyA.size(), keyB.size()));
        if (content_cmp)
            return content_cmp;

        return int(keyA.size()) - int(keyB.size());
    }
};

// This is not actually a standard Murmur3 hash and is not interchangable with the reference implementation.
// Because Cassandra is implemented in Java and Murmur3 is implemented in C there are some bugs in Cassandra's
// version around signed and unsigned int. This is a C re-implementation of Cassandra's broken Murmur3 implementation
class Murmer3Partitioner : public Partitioner
{
public:
    static int64_t fmix(int64_t k)
    {
        k ^= (uint64_t)k >> 33;
        k *= 0xff51afd7ed558ccdL;
        k ^= (uint64_t)k >> 33;
        k *= 0xc4ceb9fe1a85ec53L;
        k ^= (uint64_t)k >> 33;

        return k;
    }

    static int64_t rotl64(int64_t v, int32_t n)
    {
        return ((v << n) | ((uint64_t)v >> (64 - n)));
    }

    static int64_t getblock(const char * key, int offset, int index)
    {
        int i_8 = index << 3;
        int blockOffset = offset + i_8;
        return ((int64_t) key[blockOffset + 0] & 0xff) + (((int64_t) key[blockOffset + 1] & 0xff) << 8) +
        (((int64_t) key[blockOffset + 2] & 0xff) << 16) + (((int64_t) key[blockOffset + 3] & 0xff) << 24) +
        (((int64_t) key[blockOffset + 4] & 0xff) << 32) + (((int64_t) key[blockOffset + 5] & 0xff) << 40) +
        (((int64_t) key[blockOffset + 6] & 0xff) << 48) + (((int64_t) key[blockOffset + 7] & 0xff) << 56);
    }

    virtual void assign_token(CassandraParser::Token & token, const char * key, size_t length) const
    {
        const int32_t nblocks = int32_t(length) / 16; // Process as 128-bit blocks.
        const int64_t seed = 0;
        int32_t offset = 0;

        int64_t h1 = seed;
        int64_t h2 = seed;

        int64_t c1 = 0x87c37b91114253d5L;
        int64_t c2 = 0x4cf5ad432745937fL;

        //----------
        // body

        for(int32_t i = 0; i < nblocks; i++)
        {
            int64_t k1 = getblock(key, offset, i*2+0);
            int64_t k2 = getblock(key, offset, i*2+1);

            k1 *= c1; k1 = rotl64(k1,31); k1 *= c2; h1 ^= k1;

            h1 = rotl64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;

            k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

            h2 = rotl64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
        }

        //----------
        // tail

        // Advance offset to the unprocessed tail of the data.
        offset += nblocks * 16;

        int64_t k1 = 0;
        int64_t k2 = 0;

        switch(length & 15)
        {
            case 15: k2 ^= ((int64_t) key[offset+14]) << 48;
            case 14: k2 ^= ((int64_t) key[offset+13]) << 40;
            case 13: k2 ^= ((int64_t) key[offset+12]) << 32;
            case 12: k2 ^= ((int64_t) key[offset+11]) << 24;
            case 11: k2 ^= ((int64_t) key[offset+10]) << 16;
            case 10: k2 ^= ((int64_t) key[offset+9]) << 8;
            case  9: k2 ^= ((int64_t) key[offset+8]) << 0;
                k2 *= c2; k2  = rotl64(k2,33); k2 *= c1; h2 ^= k2;

            case  8: k1 ^= ((int64_t) key[offset+7]) << 56;
            case  7: k1 ^= ((int64_t) key[offset+6]) << 48;
            case  6: k1 ^= ((int64_t) key[offset+5]) << 40;
            case  5: k1 ^= ((int64_t) key[offset+4]) << 32;
            case  4: k1 ^= ((int64_t) key[offset+3]) << 24;
            case  3: k1 ^= ((int64_t) key[offset+2]) << 16;
            case  2: k1 ^= ((int64_t) key[offset+1]) << 8;
            case  1: k1 ^= ((int64_t) key[offset]);
                k1 *= c1; k1  = rotl64(k1,31); k1 *= c2; h1 ^= k1;
        };

        //----------
        // finalization

        h1 ^= length;
        h2 ^= length;

        h1 += h2;
        h2 += h1;

        h1 = fmix(h1);
        h2 = fmix(h2);

        h1 += h2;
        h2 += h1;
        // Emulating cassandra's behavior
        if (h1 == std::numeric_limits<int64_t>::min())
            h1 = std::numeric_limits<int64_t>::max();

        for (size_t i = 0; i < sizeof(int64_t); i++)
        {
            token[i] = ((char *)&h1)[i];
        }
    }

    virtual int compare_token(const CassandraParser::Token & tokenA, const std::string & keyA, const CassandraParser::Token & tokenB, const std::string & keyB) const
    {
        const int64_t resultA = *reinterpret_cast<const int64_t *>(&tokenA);
        const int64_t resultB = *reinterpret_cast<const int64_t *>(&tokenB);
        // To prevent wrap around, use comparisons here
        if (resultA < resultB)
            return -1;
        else if (resultA > resultB)
            return 1;

        const int content_cmp = std::memcmp(keyA.data(), keyB.data(), std::min(keyA.size(), keyB.size()));
        if (content_cmp)
            return content_cmp;

        return int(keyA.size()) - int(keyB.size());
    }
};


class ByteOrderPartitioner : public Partitioner
{
public:
    virtual void assign_token(CassandraParser::Token & token, const char * key, size_t key_length) const
    {
    }

    virtual int compare_token(const CassandraParser::Token & tokenA, const std::string & keyA, const CassandraParser::Token & tokenB, const std::string & keyB) const
    {
        const int content_cmp = std::memcmp(keyA.data(), keyB.data(), std::min(keyA.size(), keyB.size()));
        if (content_cmp)
            return content_cmp;
        return int(keyA.size()) - int(keyB.size());
    }
};


class OrderPreservingPartitioner : public Partitioner
{
public:
    virtual void assign_token(CassandraParser::Token & token, const char * key, size_t key_length) const
    {
    }

    virtual int compare_token(const CassandraParser::Token & tokenA, const std::string & keyA, const CassandraParser::Token & tokenB, const std::string & keyB) const
    {
        return keyA.compare(keyB);
    }
};

static RandomPartitioner          randomPartitioner;
static ByteOrderPartitioner       byteOrderPartitioner;
static OrderPreservingPartitioner orderPreservingPartitioner;
static Murmer3Partitioner         murmer3Partitioner;

const Partitioner * Partitioner::partitioner_from_name(const char * partitionerIdentifier)
{
    const Partitioner * const partitioners[] =
    {
        &randomPartitioner,
        &byteOrderPartitioner,
        &orderPreservingPartitioner,
        &murmer3Partitioner,
        nullptr
    };
    const char * const partitioner_names[] =
    {
        "RandomPartitioner",
        "ByteOrderedPartitioner",
        "OrderPreservingPartitioner",
        "Murmur3Partitioner",
        nullptr
    };

    constexpr char partitionerPrefix[] = "org.apache.cassandra.dht.";
    constexpr size_t prefixLen = sizeof(partitionerPrefix) - 1;
    if (std::strncmp(partitionerIdentifier, partitionerPrefix, prefixLen) == 0)
    {
        for (int i = 0; partitioner_names[i] != nullptr; i++)
        {
            if (std::strcmp(partitionerIdentifier + prefixLen, partitioner_names[i]) == 0)
            {
                return partitioners[i];
            }
        }
    }

    fprintf(stderr, "Unknown partitioner '%s'\n", partitionerIdentifier);
    return nullptr;
}

// Ancient versions of Cassandra only have the random partitioner, so this is the default
// Returns the partitioner called "RandomPartitioner". Does not return a partitioner at random.
const Partitioner * Partitioner::random_partitioner()
{
    return &randomPartitioner;
}
