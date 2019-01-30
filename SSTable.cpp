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
//  SSTable.cpp
//  Streams individual SSTables files.

#include "SSTable.hpp"
#include "Partitioners.hpp"

#include <assert.h>

#include <cstring>
#include <limits>

#define VERSION_STRING_TO_VERSION(a, b) (((a - 'a') * 26 + (b - 'a')))

#define VERSION_MA VERSION_STRING_TO_VERSION('m', 'a')
#define VERSION_LA VERSION_STRING_TO_VERSION('l', 'a')
#define VERSION_KA VERSION_STRING_TO_VERSION('k', 'a')
#define VERSION_JB VERSION_STRING_TO_VERSION('j', 'b')
#define VERSION_JA VERSION_STRING_TO_VERSION('j', 'a')
#define VERSION_IB VERSION_STRING_TO_VERSION('i', 'b')
#define VERSION_IA VERSION_STRING_TO_VERSION('i', 'a')
#define VERSION_HE VERSION_STRING_TO_VERSION('h', 'e')
#define VERSION_HD VERSION_STRING_TO_VERSION('h', 'd')
#define VERSION_HC VERSION_STRING_TO_VERSION('h', 'c')
#define VERSION_D  VERSION_STRING_TO_VERSION('d', 'a')

const char INDEX_SUFFIX[] = "-Index.db";
const char SUMMARY_SUFFIX[] = "-Summary.db";
const char COMPRESSION_INFO_SUFFIX[] = "-CompressionInfo.db";
const char DATA_SUFFIX[] = "-Data.db";

std::unique_ptr<SStable> SStable::create_table(const TableConfig & config)
{
    if (config.version >= VERSION_MA)
    {
        return std::unique_ptr<SStable>(new NewSStable(config));
    }
    else
    {
        return std::unique_ptr<SStable>(new OldSStable(config));
    }
}


bool SStable::init_at_key(const Partitioner & partitioner, const CassandraParser::Token & first_token, const std::string & first_key)
{
    UncompressedBuffer index_buffer((config.path + INDEX_SUFFIX).c_str());
    if (!index_buffer.good())
    {
        return false;
    }

    int64_t found;
    if (find_partition_in_summary(found, partitioner, config.path, first_token, first_key))
    {
        index_buffer.seek(found);
    }

    // Now use the index to find the key we are looking for
    while (!index_buffer.is_eof())
    {
        next_key_value = index_buffer.read_string();
        start_offset = config.version >= VERSION_MA ? index_buffer.read_unsigned_vint() : index_buffer.read_longlong();
        partitioner.assign_token(next_token_value, next_key_value.data(), next_key_value.length());
        if (partitioner.compare_token(first_token, first_key, next_token_value, next_key_value) <= 0)
        {
            return true;
        }

        uint64_t to_skip = config.version >= VERSION_MA ? index_buffer.read_unsigned_vint() : index_buffer.read_int();
        index_buffer.skip_bytes(to_skip);
    }
    return false;
}
bool SStable::init(const Partitioner & partitioner)
{
    if (open())
    {
        read_row(&partitioner);
        close();
        return true;
    }
    return false;
}


bool SStable::open()
{
    const CompressedBuffer::ChecksumClass checksumClass = (config.version >= VERSION_JB && config.version < VERSION_MA) ? CompressedBuffer::ADLER32 : CompressedBuffer::CRC32;
    data_buffer = std::make_shared<CompressedBuffer>((config.path + DATA_SUFFIX).c_str(),
                                                     (config.path + COMPRESSION_INFO_SUFFIX).c_str(),
                                                     checksumClass, config.version >= VERSION_JB);
    data_buffer->seek(start_offset);

    fsm = READ_ROW;
    reset();
    return data_buffer->good();
}


void SStable::close()
{
    data_buffer.reset();
}

// The summery is a seperate buffer that records a small subset of keys in order to find a starting position
// in the index faster.
// This will find the position of the index that it should start scanning at.
bool SStable::find_partition_in_summary(int64_t & found, const Partitioner & partitioner, const std::string & prefix, const CassandraParser::Token & first_token, const std::string & first_key)
{
    // If there is a summary, use it to find the key faster
    UncompressedBuffer summary_buffer((prefix + SUMMARY_SUFFIX).c_str());
    if (!summary_buffer.good())
    {
        return false;
    }

    summary_buffer.skip_bytes(4);
    int32_t size = summary_buffer.read_int();
    int32_t memSize = (int32_t)summary_buffer.read_longlong();

    if (config.version >= VERSION_KA)
        summary_buffer.skip_bytes(8);

    // Summary is designed to keep in memory, so this is safe.
    // Also, all offsets are native-endian.
    const char * toc = (const char *)summary_buffer.read_bytes(memSize);
    if (toc == nullptr)
    {
        return false;
    }

    const int32_t * index = (const int32_t *)toc;

    const uint8_t * lower_bounds = NULL;
    int32_t bottom = 0, top = size - 1;
    // Do a quick binary sort here to find the lower bounds.
    while (bottom < top)
    {
        const int32_t middle = bottom + (top - bottom) / 2;
        const int32_t offset = index[middle];
        const int32_t next_offset = middle + 1 == size ? memSize : index[middle + 1];
        assert(next_offset <= memSize);
        assert(offset < next_offset);
        const size_t len = next_offset - offset - 8;

        CassandraParser::Token token;
        partitioner.assign_token(token, toc + offset, len);
        int comp = partitioner.compare_token(first_token, first_key, token, std::string(toc + offset, len));
        if (comp >= 0)
            lower_bounds = (uint8_t *)(toc + offset + len);

        if (comp < 0)
            top = middle - 1;
        else if (comp > 0)
            bottom = middle + 1;
        else
            break;
    }

    if (lower_bounds)
    {
        found = *(int64_t *)lower_bounds;
        return true;
    }
    return false;
}

static bool isSSTableVersion(const char * versionString, const char lowerBound)
{
    // Note: this is safe for short strings as null terminators will cause a short circuit
    return versionString[0] >= lowerBound && versionString[0] <= 'z' &&
    versionString[1] >= 'a' && versionString[1] <= 'z' &&
    versionString[2] == '-';
}

int SStable::getVersionFromFilename(const char * name)
{
    // Later versions (la and above) drop the prefix and put in a suffix.
    if (isSSTableVersion(name, 'l'))
    {
        return VERSION_STRING_TO_VERSION(name[0], name[1]);
    }

    // Older versions will put the version strings in the third hyphen separated bit
    const char * foundChar = std::strchr(name, '-');
    if (foundChar == nullptr)
    {
        return -1;
    }

    foundChar = std::strchr(foundChar + 1, '-');
    if (foundChar == nullptr)
    {
        return -1;
    }

    foundChar++;

    if (isSSTableVersion(foundChar, 'a'))
    {
        return VERSION_STRING_TO_VERSION(foundChar[0], foundChar[1]);
    }

    // Ancient versions use just a single character version string
    if (foundChar[1] == '-' && foundChar[0] >= 'a' && foundChar[0] <= 'd')
    {
        return VERSION_STRING_TO_VERSION(foundChar[0], 'a');
    }

    return -1;
}

bool SStable::extractKeyspaceAndTable(int version, const std::string& fileName, const std::string& dir_string,
                                      std::string& thisKeyspace, std::string& thisTable)
{
    if (version < VERSION_LA)
    {
        // Older versions store keyspace and table in the filename
        size_t dashPos1 = fileName.find("-");
        if (dashPos1 == std::string::npos)
            return false;

        size_t dashPos2 = fileName.find("-", dashPos1 + 1);
        if (dashPos2 == std::string::npos)
            return false;

        thisKeyspace = fileName.substr(0, dashPos1);
        thisTable = fileName.substr(dashPos1 + 1, dashPos2 - dashPos1 - 1);
    }
    else
    {
        // Newer versions store keyspace and table in the path
        const char * thisSeperator = dir_string.c_str();
        const char * lastSeperator = nullptr;
        const char * previousSeperator = nullptr;
        while (const char * nextSeperator = strchr(thisSeperator, '/'))
        {
            previousSeperator = lastSeperator;
            lastSeperator = thisSeperator;
            thisSeperator = nextSeperator + 1;
        }

        if (previousSeperator == nullptr || lastSeperator == nullptr)
        {
            return false;
        }

        thisKeyspace = dir_string.substr(previousSeperator - dir_string.c_str(), lastSeperator - previousSeperator - 1);
        thisTable = dir_string.substr(lastSeperator - dir_string.c_str(), thisSeperator - lastSeperator - 1);
    }

    return true;
}

static void skip_histogram(Buffer & buffer)
{
    int32_t num_cols = buffer.read_int();
    buffer.skip_bytes(num_cols * 2 * 8);
}

const Partitioner * SStable::read_metadata(Buffer & buf, int version, TableSchema & schema)
{
    if (version >= VERSION_KA)
    {
        enum MetaDataTOC
        {
            META_DATA_VALIDATION = 0,
            META_DATA_COMPACTION = 1,
            META_DATA_STATS = 2,
            META_DATA_HEADER = 3,
        };

        int32_t num_components = buf.read_int();
        int32_t validation_offset = -1;
        int32_t header_offset = -1;
        for (int i = 0; i < num_components; i++)
        {
            const int32_t this_type = buf.read_int();
            const int32_t this_offset = buf.read_int();
            if (this_type == META_DATA_VALIDATION)
                validation_offset = this_offset;
            else if (this_type == META_DATA_HEADER)
                header_offset = this_offset;
        }

        if (header_offset >= 0)
        {
            buf.seek(header_offset);
            schema.parse(buf);
        }

        if (validation_offset < 0)
            return nullptr;

        buf.seek(validation_offset);
        return Partitioner::partitioner_from_name(buf.read_string().c_str());
    }
    else if (version >= VERSION_JA)
    {
        skip_histogram(buf);
        skip_histogram(buf);
        buf.skip_bytes(5 * 8 + 2 * 4);
        return Partitioner::partitioner_from_name(buf.read_string().c_str());
    }
    else if (version >= VERSION_HC)
    {
        skip_histogram(buf);
        skip_histogram(buf);
        buf.skip_bytes(8 + 4);
        if (version >= VERSION_IB)
            buf.skip_bytes(8);
        if (version >= VERSION_HD)
            buf.skip_bytes(8);
        buf.skip_bytes(8);
        return Partitioner::partitioner_from_name(buf.read_string().c_str());
    }
    else
    {
        return Partitioner::random_partitioner();
    }
}

bool OldSStable::read_row(const Partitioner * pPartitioner)
{
    assert(fsm == READ_ROW);

    next_key_value = data_buffer->read_string();
    if (data_buffer->is_eof())
        return true;

    if (pPartitioner)
        pPartitioner->assign_token(next_token_value, next_key_value.data(), next_key_value.length());

    if (config.version < VERSION_D)
        data_buffer->skip_bytes(4); // size
    else if (config.version < VERSION_JA)
        data_buffer->skip_bytes(8); // size

    data_buffer->skip_bytes(4); /* local_deletion */
    row_marked_for_deletion = data_buffer->read_longlong();

    if (config.version < VERSION_JA)
        remaining_columns = data_buffer->read_int();

    fsm = READ_COLUMN;
    read_column();
    return data_buffer->is_eof();
}


bool OldSStable::read_column()
{
    if (fsm == READ_COLUMN_DATA)
    {
        data_buffer->skip_data();
        fsm = READ_COLUMN;
    }

    assert(fsm == READ_COLUMN);

    next_column_info.clear_flags();

    if (config.version < VERSION_JA)
    {
        // below ja uses a column count.
        if (remaining_columns > 0)
        {
            remaining_columns--;
        }
        else
        {
            next_column_info.name.clear();
            return false;
        }
    }

    // ja and above use an empty column to terminate row.
    next_column_info.name = data_buffer->read_string();

    if (next_column_info.name.empty())
    {
        fsm = READ_ROW; // No more data in this row!
        return false;
    }

    // This might be a compound path or a clustering path. As we support neither, just take the name itself.
    for (size_t buffer_len = next_column_info.name.length(); buffer_len >= 2; )
    {
        const size_t advanced = next_column_info.name.length() - buffer_len;
        const uint8_t * bytes = reinterpret_cast<const uint8_t *>(next_column_info.name.data()) + advanced;

        const uint16_t len = (bytes[0] << 8) | bytes[1];
        if (buffer_len > len + 3u)
        {
            // TODO: next_column_info.name.substr(advanced + 2u, len) is the path element
            buffer_len -= len + 3u;
        }
        else
        {
            if (buffer_len == len + 3u)
            {
                next_column_info.name = next_column_info.name.substr(advanced + 2u, len);
            }
            break;
        }
    }

    uint8_t flags = data_buffer->read_byte();
    next_column_info.deleted = (flags & DELETION_MASK) != 0;
    if (flags & RANGE_TOMBSTONE_MASK)
    {
        next_column_info.data = data_buffer->read_string();
        data_buffer->skip_bytes(4); /* local_deletion */
        next_column_info.ts = data_buffer->read_longlong();
        next_column_info.range_tombstone = true;
        // Leave FSM as READ_COLUMN
    }
    else
    {
        if (flags & COUNTER_MASK)
        {
            next_column_info.extra_data.counter_timestamp = data_buffer->read_longlong();
        }
        else if (flags & EXPIRATION_MASK)
        {
            next_column_info.extra_data.expiration.ttl = data_buffer->read_int();
            next_column_info.extra_data.expiration.expiration = data_buffer->read_int();
            next_column_info.expiring = true;
        }
        next_column_info.ts = data_buffer->read_longlong();
        fsm = READ_COLUMN_DATA; // Must read data!
    }
    return true;
}


bool OldSStable::read_column_data(std::string & data)
{
    assert(fsm == READ_COLUMN_DATA);
    bool result = data_buffer->read_data(data);
    fsm = READ_COLUMN;
    return result;
}

void NewSStable::decode_column_subset(Buffer & buf, std::vector<bool> & subset, size_t n_columns)
{
    uint64_t encoded = buf.read_unsigned_vint();
    if (encoded == 0)
    {
        subset.assign(n_columns, true);
    }
    else if (n_columns >= 64)
    {
        size_t column_count = n_columns - encoded;
        const bool is_positive = column_count < (n_columns / 2);
        subset.assign(n_columns, !is_positive);
        for (size_t i = 0; i < column_count; i++)
        {
            subset[buf.read_unsigned_vint()] = is_positive;
        }
    }
    else
    {
        subset.resize(n_columns);
        for (size_t i = 0; i < n_columns; i++)
        {
            subset[i] = (encoded & 1) != 0;
            encoded >>= 1;
        }
    }
}

void NewSStable::read_clustering_columns(size_t size)
{
    // This reads the clustering headers out. They are not currently in use.
    for (size_t clusteringColumn = 0; clusteringColumn < size; )
    {
        uint64_t clusteringHeader = data_buffer->read_unsigned_vint();
        size_t limit = std::min(config.schema.clustering.size(), clusteringColumn + 32);
        for (int shift = 0; clusteringColumn < limit; clusteringColumn++, shift += 2)
        {
            if ((clusteringHeader & (3 << shift)) == 0)
            {
                size_t skip = TableSchema::get_column_size(config.schema.clustering[clusteringColumn], *data_buffer);
                data_buffer->skip_bytes(skip);
            }
        }
    }
}

bool NewSStable::read_row(const Partitioner * pPartitioner)
{
    if (at_end_of_partition)
    {
        next_key_value = data_buffer->read_string();
        if (data_buffer->is_eof())
            return true;
        data_buffer->skip_bytes(4); /* local_deletion */
        partition_marked_for_deletion = data_buffer->read_longlong();

        if (pPartitioner)
            pPartitioner->assign_token(next_token_value, next_key_value.data(), next_key_value.length());

        at_end_of_partition = false;
    }

    uint8_t flags = data_buffer->read_byte();
    if (flags & END_OF_PARTITION)
    {
        at_end_of_partition = true;
        return read_row(pPartitioner);
    }

    uint8_t extended_flags = (flags & EXTENSION_FLAG) ? data_buffer->read_byte() : 0;
    is_static = (extended_flags & IS_STATIC) != 0;

    if (flags & IS_MARKER)
    {
        return read_marker();
    }
    else
    {
        return read_normal_row(flags);
    }
}

bool NewSStable::read_marker()
{
    uint8_t type = data_buffer->read_byte();
    uint16_t size = data_buffer->read_short();
    if (!is_static)
    {
        read_clustering_columns(size);
    }

    data_buffer->read_unsigned_vint(); // rowsize (not needed)
    data_buffer->read_unsigned_vint(); // previous unfiltered size (not needed)

    row_marked_for_deletion = data_buffer->read_longlong();
    data_buffer->skip_bytes(4); // local deletion (not needed)
    enum
    {
        EXCL_END_INCL_START_BOUNDARY = 2,
        INCL_END_EXCL_START_BOUNDARY = 5
    };
    if (type == EXCL_END_INCL_START_BOUNDARY || type == INCL_END_EXCL_START_BOUNDARY)
    {
        // These types have an additional deletion time, but we don't use it.
        data_buffer->skip_bytes(12);
    }
    next_column_info.clear_flags();
    next_column_info.range_tombstone = true;
    fsm = READ_COLUMN;
    columns_present.clear();
    this_column_index = 0;
    return data_buffer->is_eof();
}

bool NewSStable::read_normal_row(uint8_t flags)
{
    if (!is_static)
    {
        read_clustering_columns(config.schema.clustering.size());
    }

    data_buffer->read_unsigned_vint(); // rowsize (not needed)
    data_buffer->read_unsigned_vint(); // previous unfiltered size (not needed)

    row_ttl = std::numeric_limits<uint64_t>::max();
    row_timestamp = 0;
    if (flags & HAS_TIMESTAMP)
    {
        row_timestamp = data_buffer->read_unsigned_vint() + config.schema.minTimestamp;
        if (flags & HAS_TTL)
        {
            row_ttl = data_buffer->read_unsigned_vint() + config.schema.minTTL;
            data_buffer->read_unsigned_vint(); // localDeletionTime (not needed)
        }
    }

    if (flags & HAS_DELETION)
    {
        row_marked_for_deletion = data_buffer->read_unsigned_vint() + config.schema.minTimestamp;
        data_buffer->read_unsigned_vint(); // localDeletionTime (not needed)
    }
    else
    {
        row_marked_for_deletion = partition_marked_for_deletion;
    }

    const std::vector<std::pair<std::string, TableSchema::ColumnFormat>> & columns = is_static ? config.schema.static_columns : config.schema.regular_columns;
    if (flags & HAS_ALL_COLUMNS)
    {
        columns_present.assign(columns.size(), true);
    }
    else
    {
        decode_column_subset(*data_buffer, columns_present, columns.size());
    }

    this_column_index = 0;
    next_column();

    read_column();
    return data_buffer->is_eof();
}

bool NewSStable::read_column()
{
    enum
    {
        IS_DELETED_MASK             = 0x01,
        IS_EXPIRING_MASK            = 0x02,
        HAS_EMPTY_VALUE_MASK        = 0x04,
        USE_ROW_TIMESTAMP_MASK      = 0x08,
        USE_ROW_TTL_MASK            = 0x10
    };

    if(fsm == READ_COLUMN_DATA)
    {
        std::string ignore;
        read_column_data(ignore);
    }

    assert(fsm == READ_COLUMN);

    next_column_info.clear_flags();
    if (this_column_index >= columns_present.size())
    {
        fsm = READ_ROW;
        next_column_info.name.clear();
        return false;
    }

    const std::vector<std::pair<std::string, TableSchema::ColumnFormat>> & columns = is_static ? config.schema.static_columns : config.schema.regular_columns;
    next_column_info.name = columns[this_column_index].first;

    uint8_t flags = data_buffer->read_byte();
    if (flags & USE_ROW_TIMESTAMP_MASK)
    {
        next_column_info.ts = row_timestamp;
    }
    else
    {
        next_column_info.ts = data_buffer->read_unsigned_vint() + config.schema.minTimestamp;
    }

    next_column_info.deleted = (flags & IS_DELETED_MASK) != 0;
    next_column_info.expiring = (flags & IS_EXPIRING_MASK) != 0;
    if (flags & USE_ROW_TTL_MASK)
    {
        next_column_info.expiring = row_ttl != std::numeric_limits<uint64_t>::max();
        next_column_info.extra_data.expiration.ttl = static_cast<uint32_t>(row_ttl);
    }
    else
    {
        if (next_column_info.expiring || next_column_info.deleted)
        {
            data_buffer->read_unsigned_vint(); // local deletion time
        }
        if (next_column_info.expiring)
        {
            next_column_info.extra_data.expiration.ttl = static_cast<uint32_t>(data_buffer->read_unsigned_vint() + config.schema.minTTL);
        }
    }

    if ((flags & HAS_EMPTY_VALUE_MASK) == 0)
    {
        fsm = READ_COLUMN_DATA;
    }
    return true;
}

bool NewSStable::read_column_data(std::string & data)
{
    if (fsm == READ_COLUMN)
    {
        data.clear();
    }
    else
    {
        const std::vector<std::pair<std::string, TableSchema::ColumnFormat>> & columns = is_static ? config.schema.static_columns : config.schema.regular_columns;
        size_t size = config.schema.get_column_size(columns[this_column_index].second, *data_buffer);
        const uint8_t * bytes = data_buffer->read_bytes(size);
        if (bytes)
        {
            data.assign(reinterpret_cast<const char *>(bytes), size);
        }

        this_column_index++;
        next_column();
    }
    return true;
}

void NewSStable::next_column()
{
    while (this_column_index < columns_present.size() && !columns_present[this_column_index])
    {
        this_column_index++;
    }
    fsm = READ_COLUMN;
}
