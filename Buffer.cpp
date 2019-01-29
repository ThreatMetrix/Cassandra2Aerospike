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
//  Buffer.cpp
//  Abstraction for Cassandra DB buffers, including decompression

#include "Buffer.hpp"
#include "lz4.h"
#include "snappy.h"

#include <alloca.h>
#include <assert.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdlib.h>
#include <zlib.h>

#include <cstring>
#include <iostream>

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define be64toh(x) OSSwapBigToHostInt64(x)
#define be32toh(x) OSSwapBigToHostInt32(x)
#define be16toh(x) OSSwapBigToHostInt16(x)
#else
#include <sys/endian.h>
#endif

template<class T>
static T read_from_buffer(Buffer & b)
{
    const uint8_t * data = b.read_bytes(sizeof(T));
    if (data)
    {
        return *reinterpret_cast<const T*>(data);
    }
    return 0;
}

int32_t Buffer::read_int()
{
    return static_cast<int32_t>(be32toh(read_from_buffer<uint32_t>(*this)));
}

uint64_t Buffer::read_unsigned_vint()
{
    const uint8_t * firstByte = read_bytes(1);
    if (firstByte == nullptr)
    {
        return 0;
    }

    if (*firstByte < 0x7f)
    {
        return *firstByte;
    }

    int extraBytes = 0;
    // Count the number of leading ones in the first byte to find out how many subsequent bytes are needed.
    for (; extraBytes < 8 && (*firstByte & (0x80 >> extraBytes)) != 0; extraBytes++)
        ;

    // The leading ones are masked away
    uint64_t retval = *firstByte & (0xff >> extraBytes);
    const uint8_t * data = read_bytes(extraBytes);
    if (data == nullptr)
    {
        return 0;
    }

    for (int i = 0; i < extraBytes; i++)
    {
        retval <<= 8;
        retval |= data[i];
    }

    return retval;
}

int64_t Buffer::read_vint()
{
    // This is a zig-zag encoded signed integer
    int64_t n = read_unsigned_vint();
    return (n << 1) ^ (n >> 63);
}

int16_t Buffer::read_short()
{
    return static_cast<int16_t>(be16toh(read_from_buffer<uint16_t>(*this)));
}

uint8_t Buffer::read_byte()
{
    return *read_bytes(1);
}

std::string Buffer::read_string()
{
    int16_t len = read_short();
    if (is_eof())
        return "";

    const uint8_t * data = read_bytes(len);
    if (data == NULL)
        return "";

    return std::string((const char*)data, (size_t)len);
}

std::string Buffer::read_vint_length_string()
{
    int64_t len = read_unsigned_vint();
    if (is_eof())
        return "";

    const uint8_t * data = read_bytes(len);
    if (data == NULL)
        return "";

    return std::string((const char*)data, (size_t)len);
}

int64_t Buffer::read_longlong()
{
    return static_cast<int64_t>(be64toh(read_from_buffer<uint64_t>(*this)));
}

float   Buffer::read_float()
{
    return read_from_buffer<float>(*this);
}

double  Buffer::read_double()
{
    return read_from_buffer<double>(*this);
}

bool Buffer::read_data(std::string & read)
{
    int32_t len = read_int();
    if (is_eof())
        return false;

    const uint8_t * data = read_bytes(len);
    if (data == NULL)
        return false;

    read.assign((const char*)data, (size_t)len);
    return true;
}

void Buffer::skip_data()
{
    int32_t len = read_int();
    skip_bytes(len);
}

const uint8_t * UncompressedBuffer::read_bytes(size_t n_bytes)
{
    if (n_bytes > buffer_len)
    {
        if (buffer)
            delete[] buffer;
        buffer = new uint8_t[n_bytes];
        buffer_len = n_bytes;
    }
    if (fread(buffer, n_bytes, 1, fp) == 0)
    {
        iseof = true;
        return NULL;
    }
    return buffer;
}

void UncompressedBuffer::skip_bytes(size_t n_bytes)
{
    fseek(fp, n_bytes, SEEK_CUR);
}

void UncompressedBuffer::seek(int64_t pos)
{
    iseof = fseeko(fp, pos, SEEK_SET) != 0;
}

UncompressedBuffer::UncompressedBuffer(const char * filename) : fp(NULL), buffer(NULL), buffer_len(0), iseof(false)
{
    fp = fopen(filename, "r");
}

UncompressedBuffer::~UncompressedBuffer()
{
    if (fp != NULL)
    {
        fclose(fp);
    }
    if (buffer)
    {
        delete[] buffer;
    }
}


bool CompressedBuffer::s_enableChecksum = true;

void CompressedBuffer::adjust_buffer(size_t min_length, size_t useful_bytes_in_buffer, size_t useless_bytes_in_buffer)
{
    if (min_length > buffer_allocation)
    {
        uint8_t * new_buffer = new uint8_t[min_length];
        if (useful_bytes_in_buffer)
        {
            memcpy(new_buffer, buffer + useless_bytes_in_buffer, useful_bytes_in_buffer);
        }
        if (buffer)
        {
            delete[] buffer;
        }
        buffer = new_buffer;
        buffer_allocation = min_length;
    }
    else if (useful_bytes_in_buffer)
    {
        memmove(buffer, buffer + useless_bytes_in_buffer, useful_bytes_in_buffer);
    }
    buffer_len = min_length;
}

void CompressedBuffer::decompress_block(const uint8_t * read_chunk, uint8_t * write_chunk, int chunk_size)
{
    switch (m_compressionClass)
    {
        case SnappyCompressor:
            snappy::RawUncompress((const char *)read_chunk, chunk_size, (char *)write_chunk);
            break;

        case LZ4Compressor:
        {
            const uint32_t * block_len = (uint32_t *)(read_chunk);
            LZ4_decompress_fast((const char *)(block_len + 1), (char *)write_chunk, *block_len);
        }
            break;

        case DeflateCompressor:
        {
            z_stream infstream;
            memset(&infstream, 0, sizeof(infstream));
            infstream.avail_in = chunk_size;
            infstream.next_in = const_cast<uint8_t *>(read_chunk);
            infstream.avail_out = chunk_len;
            infstream.next_out = write_chunk;
            inflateInit(&infstream);
            inflate(&infstream, Z_NO_FLUSH);
            inflateEnd(&infstream);
            break;
        }
    }
}

bool CompressedBuffer::verify_checksum(const uint8_t * data, uint32_t data_len, uint8_t * checksum_data,
                                       const uint64_t start_of_this_read, const uint64_t end_of_this_read)
{
    if (!s_enableChecksum)
    {
        return true;
    }

    const uint32_t calculated_checksum = checksum_class == CRC32 ?
            (uint32_t)crc32(checksum_start, data, data_len) :
            (uint32_t)adler32(checksum_start, data, data_len);

    const uint32_t checksum = checksum_data[0] << 24 | checksum_data[1] << 16 | checksum_data[2] << 8 | checksum_data[3];

    if (checksum == calculated_checksum)
    {
        return true;
    }

    fprintf(stderr, "Checksum mismatch at %s %lld - %lld %x %x\n", filename.c_str(), start_of_this_read, end_of_this_read, checksum, calculated_checksum);
    return false;
}

const uint8_t * CompressedBuffer::read_bytes(size_t n_bytes)
{
    const int64_t last_byte_required = file_offset + n_bytes;
    if (last_byte_required > uncompressed_len)
    {
        iseof = true;
        return NULL;
    }

    const int64_t last_byte_in_buffer = buffer_offset + buffer_len;
    if (file_offset < buffer_offset || last_byte_required > last_byte_in_buffer)
    {
        size_t last_chunk = (last_byte_required + chunk_len - 1) / chunk_len;

        size_t first_chunk_to_read = file_offset / chunk_len;
        size_t useful_bytes_in_buffer = 0;
        if (file_offset >= buffer_offset && file_offset <= last_byte_in_buffer)
        {
            first_chunk_to_read = last_byte_in_buffer / chunk_len;
            useful_bytes_in_buffer = last_byte_in_buffer - file_offset;
            assert(file_offset + useful_bytes_in_buffer == first_chunk_to_read * chunk_len);
        }
        size_t min_length = (last_chunk - first_chunk_to_read) * chunk_len + useful_bytes_in_buffer;
        adjust_buffer(min_length, useful_bytes_in_buffer, buffer_len - useful_bytes_in_buffer);

        buffer_offset = first_chunk_to_read * chunk_len - useful_bytes_in_buffer;
        const int64_t start_of_read = offsets[first_chunk_to_read];
        int64_t end_of_read;
        if (last_chunk < offsets.size())
        {
            end_of_read = offsets[last_chunk];
        }
        else
        {
            end_of_read = lseek(fd, 0, SEEK_END);
        }

        const uint64_t read_len = end_of_read - start_of_read;
        uint8_t * read_buffer = (uint8_t *)alloca(read_len);
        pread(fd, read_buffer, read_len, start_of_read);
        for (size_t i = first_chunk_to_read; i < last_chunk; i++)
        {
            const int64_t start_of_this_read = offsets[i];
            const int64_t end_of_this_read = (i + 1 == last_chunk) ? end_of_read : offsets[i + 1];
            int chunk_size = int(end_of_this_read - start_of_this_read - 4 /* Checksum */);

            const size_t buffer_read_pos = (i - first_chunk_to_read) * chunk_len + useful_bytes_in_buffer;
            uint8_t * read_chunk = read_buffer + start_of_this_read - start_of_read;

            if (check_before_decompression == true)
            {
                if (!verify_checksum(read_chunk, chunk_size, read_chunk + chunk_size, start_of_this_read, end_of_this_read))
                {
                    exit(-1);
                }
            }

            decompress_block(read_chunk, buffer + buffer_read_pos, chunk_size);

            if (check_before_decompression == false)
            {
                if (!verify_checksum(buffer + buffer_read_pos,
                                     (uint32_t)std::min(int64_t(uncompressed_len - (buffer_offset + buffer_read_pos)), (int64_t)chunk_len),
                                     read_chunk + chunk_size, start_of_this_read, end_of_this_read))
                {
                    exit(-1);
                }
            }
        }
    }

    const uint8_t * start = buffer + file_offset - buffer_offset;
    file_offset += n_bytes;
    return start;
}

void CompressedBuffer::skip_bytes(size_t n_bytes)
{
    file_offset += n_bytes;
}

CompressedBuffer::CompressedBuffer(const char * filename, const char * ci_filename, ChecksumClass checksum, bool checksum_compressed) :
    fd(-1),
    iseof(false),
    buffer(NULL),
    buffer_len(0),
    buffer_allocation(0),
    buffer_offset(0),
    file_offset(0),
    checksum_class(checksum),
    check_before_decompression(checksum_compressed),
    checksum_start(checksum == ADLER32 ? (uint32_t)adler32(0L, NULL, 0) : (uint32_t)crc32(0L, NULL, 0)),
    filename(filename)
{
    UncompressedBuffer compression_info(ci_filename);
    if (compression_info.good())
    {
        std::string classname = compression_info.read_string();
        if (classname == "SnappyCompressor")
            m_compressionClass = SnappyCompressor;
        else if (classname == "LZ4Compressor")
            m_compressionClass = LZ4Compressor;
        else if (classname == "DeflateCompressor")
            m_compressionClass = DeflateCompressor;
        else
        {
            fprintf(stderr, "Unknown compression algorithm %s\n", classname.c_str());
            return;
        }

        size_t param_count = compression_info.read_int();
        for (size_t i = 0; i < param_count; i++)
        {
            compression_info.read_string();
            compression_info.read_string();
        }
        chunk_len = compression_info.read_int();
        uncompressed_len = compression_info.read_longlong();

        offsets.resize(compression_info.read_int());
        for (size_t i = 0; i < offsets.size(); i++)
        {
            offsets[i] = compression_info.read_longlong();
        }

        fd = open(filename, O_RDONLY);
    }
}

CompressedBuffer::~CompressedBuffer()
{
    close(fd);
    if (buffer)
    {
        delete[] buffer;
    }
}

bool CompressedBuffer::good() const
{
    return fd >= 0;
}

