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
//  Buffer.h
//  Abstraction for Cassandra DB buffers, including decompression

#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <stdio.h>
#include <stdint.h>
#include <string>
#include <vector>

class Buffer
{
protected:
    ~Buffer() {}

public:
    virtual const uint8_t * read_bytes(size_t n_bytes) = 0;
    virtual void skip_bytes(size_t n_bytes) = 0;
    virtual void seek(int64_t position) = 0;
    virtual bool is_eof() const = 0;
    virtual bool good() const = 0;
    int32_t read_int();
    int64_t read_vint();
    uint64_t read_unsigned_vint();
    int16_t read_short();
    uint8_t read_byte();
    int64_t read_longlong();
    float   read_float();
    double  read_double();
    std::string read_string();
    std::string read_vint_length_string();
    bool    read_data(std::string & read);
    void    skip_data();
};

class UncompressedBuffer : public Buffer
{
protected:
    FILE * fp;
    uint8_t * buffer;
    size_t buffer_len;
    bool iseof;
    UncompressedBuffer(const UncompressedBuffer & other) = delete;
    UncompressedBuffer operator=(const UncompressedBuffer & other) = delete;
public:
    virtual const uint8_t * read_bytes(size_t n_bytes) override;
    virtual void skip_bytes(size_t n_bytes) override;
    virtual void seek(int64_t position) override;
    virtual bool is_eof() const override
    {
        return iseof;
    }
    virtual bool good() const override
    {
        return fp != NULL;
    }
    UncompressedBuffer(const char * filename);
    ~UncompressedBuffer();
};

class CompressedBuffer : public Buffer
{
    CompressedBuffer(const CompressedBuffer & other) = delete;
    CompressedBuffer operator=(const CompressedBuffer & other) = delete;
public:
    enum ChecksumClass
    {
        ADLER32,
        CRC32,
        NONE
    };

    virtual const uint8_t * read_bytes(size_t n_bytes) override;
    virtual void skip_bytes(size_t n_bytes) override;
    virtual bool good() const override;
    virtual bool is_eof() const override
    {
        return iseof;
    }
    virtual void seek(int64_t position) override
    {
        file_offset = position;
    }

    CompressedBuffer(const char * filename, const char * ci_filename, ChecksumClass adler, bool checksumCompressed);
    ~CompressedBuffer();

    static void enableChecksum(bool enabled)
    {
        s_enableChecksum = enabled;
    }
protected:
    static bool s_enableChecksum;

    int fd;
    bool iseof;
    int32_t chunk_len;
    int64_t uncompressed_len;
    std::vector<int64_t> offsets;

    uint8_t * buffer;
    size_t buffer_len;
    size_t buffer_allocation;
    int64_t buffer_offset;
    int64_t file_offset;
    const ChecksumClass checksum_class;
    const bool check_before_decompression;
    const uint32_t checksum_start;
    const std::string filename;

    enum CompressionClass
    {
        LZ4Compressor,
        SnappyCompressor,
        DeflateCompressor
    };
    CompressionClass m_compressionClass;
    void adjust_buffer(size_t min_length, size_t useful_bytes_in_buffer, size_t useless_bytes_in_buffer);
    void decompress_block(const uint8_t * read_chunk, uint8_t * write_chunk, int chunk_size);
    bool verify_checksum(const uint8_t * data, uint32_t data_len, uint8_t * checksum_data,
                         const uint64_t start_of_this_read, const uint64_t end_of_this_read);
};

#endif
