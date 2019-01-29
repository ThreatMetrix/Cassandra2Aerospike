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
//  SSTable.hpp
//  Streams individual SSTables files.

#ifndef SSTable_hpp
#define SSTable_hpp

#include "Buffer.hpp"
#include "CassandraParser.hpp"
#include "Partitioners.hpp"

class Partitioner;
struct TableConfig;

class SStable
{
protected:
    std::shared_ptr<Buffer> data_buffer;

    std::string next_key_value;
    CassandraParser::Token next_token_value;

    int64_t row_marked_for_deletion;
    int64_t start_offset;

    CassandraParser::ColumnInfo next_column_info; // Data member should ALWAYS be empty
    enum FSM
    {
        READ_ROW,
        READ_COLUMN,
        READ_COLUMN_DATA
    };
    FSM fsm;
    const TableConfig & config;
public:
    static const int64_t STILL_ACTIVE = 0x8000000000000000;

    SStable(const TableConfig & c) : row_marked_for_deletion(0), start_offset(0), fsm(READ_ROW), config(c)
    {
    }
    virtual ~SStable()
    {}
    virtual void reset()
    {
    }

    static std::unique_ptr<SStable> create_table(const TableConfig & config);
    static bool extractKeyspaceAndTable(int version, const std::string& fileName, const std::string& dir_string,
                                        std::string& thisKeyspace, std::string& thisTable);
    static int getVersionFromFilename(const char * name);
    static const Partitioner * read_metadata(Buffer & buf, int version, TableSchema & schema);
    bool init_at_key(const Partitioner & partitioner, const CassandraParser::Token & first_token, const std::string & first_key);
    bool init(const Partitioner & partitioner);
    bool open();
    void close();
    bool find_partition_in_summary(int64_t & found, const Partitioner & partitioner, const std::string & prefix, const CassandraParser::Token & first_token, const std::string & first_key);

    const CassandraParser::Token & next_token() const { return next_token_value; }
    const std::string & next_key() const { return next_key_value; }
    int64_t marked_for_deletion() const { return row_marked_for_deletion; }
    const CassandraParser::ColumnInfo & next_column() const { return next_column_info; }

    virtual bool read_row(const Partitioner * pPartitioner) = 0;
    virtual bool read_column() = 0;
    virtual bool read_column_data(std::string & data) = 0;
    virtual std::unique_ptr<SStable> duplicate() = 0;
};

// This is the format in use before MA
class OldSStable : public SStable
{
    enum
    {
        LIVE_MASK            = 0x00,
        DELETION_MASK        = 0x01,
        EXPIRATION_MASK      = 0x02,
        COUNTER_MASK         = 0x04,
        COUNTER_UPDATE_MASK  = 0x08,
        RANGE_TOMBSTONE_MASK = 0x10
    };
    size_t remaining_columns; // Note: This is only valid for ancient versions
public:
    OldSStable(const TableConfig & config) : SStable(config),
    remaining_columns(0)
    {}
    bool read_row(const Partitioner * pPartitioner) override;
    bool read_column() override;
    bool read_column_data(std::string & data) override;
    std::unique_ptr<SStable> duplicate() override
    {
        return std::unique_ptr<SStable>(new OldSStable(*this));
    }
};

// This is the format in use in MA and beyond
class NewSStable : public SStable
{
    enum Flags
    {
        END_OF_PARTITION     = 0x01,
        IS_MARKER            = 0x02,
        HAS_TIMESTAMP        = 0x04,
        HAS_TTL              = 0x08,
        HAS_DELETION         = 0x10,
        HAS_ALL_COLUMNS      = 0x20,
        HAS_COMPLEX_DELETION = 0x40,
        EXTENSION_FLAG       = 0x80
    };

    bool at_end_of_partition;
    int64_t partition_marked_for_deletion;
    uint64_t row_timestamp;
    uint64_t row_ttl;
    std::vector<bool> columns_present;
    size_t this_column_index;
    static void decode_column_subset(Buffer & buf, std::vector<bool> & subset, size_t n_columns);
    void next_column();
    void read_clustering_columns(size_t size);
    virtual void reset() override
    {
        at_end_of_partition = true;
    }
public:
    NewSStable(const TableConfig & config) : SStable(config), at_end_of_partition(true)
    {}
    bool read_row(const Partitioner * pPartitioner) override;
    bool read_marker();
    bool read_normal_row(uint8_t flags);
    bool read_column() override;
    bool read_column_data(std::string & data) override;
    std::unique_ptr<SStable> duplicate() override
    {
        return std::unique_ptr<SStable>(new NewSStable(*this));
    }
};


#endif /* SSTable_hpp */
