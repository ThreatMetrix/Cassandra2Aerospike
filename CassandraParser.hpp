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
//  CassandraParser.h
//  An iterator class to perform a for-each loop on an on-disk cassandra table

#ifndef parse_cassandra_h
#define parse_cassandra_h

#include "Buffer.hpp"
#include "SSTableSchema.hpp"

#include <set>
#include <map>
#include <memory>
#include <vector>

class Partitioner;
class SStable;

struct TableConfig
{
    TableConfig(const std::string & p, int v) : path(p), version(v) {}
    const std::string path;
    const int version;
    TableSchema schema;
};

class CassandraParser
{
public:
    // This should be long enough for the longest token.
    typedef uint8_t Token[16];

    class DatabaseRow
    {
    public:
        virtual void new_row(const std::string & key) = 0;
        virtual void new_column(const std::string & column_name, const std::string & column_value, int64_t ts) = 0;
        virtual void new_column_with_ttl(const std::string & column_name, const std::string & column_value, int64_t ts, uint32_t ttl,                               uint32_t ttlTimestampSecs) = 0;
    };
    
    struct ColumnInfo
    {
        ColumnInfo() : deleted(false), expiring(false), range_tombstone(false), ts(0LL) { extra_data.counter_timestamp = 0LL; }
        bool deleted;
        bool expiring;
        bool range_tombstone;
        std::string name;
        int64_t ts;
        union
        {
            int64_t counter_timestamp;
            struct { int32_t ttl; int32_t expiration; } expiration;
        } extra_data;
        std::string data;
        void clear_flags()
        {
            deleted = expiring = range_tombstone = false;
        }
    };

    CassandraParser() : m_totalFileSize(0), m_numFiles(0) {}

    off_t getTotalFileSize() const { return m_totalFileSize; }
    off_t getNumFiles() const { return m_numFiles; }
    const std::string& getKeyspace() const { return m_keyspace; }
    const std::string& getTableName() const { return m_tableName; }

    bool open(const std::vector<std::string> & path);
    bool add_data_file(const std::string & dir_string, const char * name, size_t namelen);

    class iterator
    {
        const CassandraParser &         m_parser;
        size_t                          m_next_table;
        std::set<size_t>                m_active_tables;
        std::vector<std::unique_ptr<SStable>> m_tables;
        size_t                          m_skippedRecords;
        size_t                          m_cassandraReadRecords;
#ifdef DEBUG
        Token                           m_last_token;
        std::string                     m_last_key;
#endif
        bool match_table(size_t * matches, size_t & n_matches, size_t index);
        void activate_table(size_t index);
        void deactivate_table(size_t index);

        size_t find_first_row_matches(size_t * matches);
        size_t find_first_column_matches(size_t * matched_columns, const size_t * matches, const size_t n_matches);

        void update_tombstones(std::map<std::string, int64_t> &tombstones, int64_t & minTime,
                                const size_t * matches, const size_t n_matches,
                                const int64_t marked_for_deletion,
                                const std::string & name) const;
        SStable & choose_latest_match(const size_t * matched_columns, const size_t column_matches);

        bool next_record(DatabaseRow & row);
    public:

        iterator(const CassandraParser & parser, std::vector<std::unique_ptr<SStable>> && tables);
        iterator(const iterator & other);
        ~iterator();

        size_t getSkippedRecords() const { return m_skippedRecords; }
        size_t getCassandraReadRecords() const { return m_cassandraReadRecords; }

        bool next(DatabaseRow & row);
        bool get_next_key(std::string & next_key);
    };

    iterator find(const std::string & primaryKey) const;
    iterator begin() const;
private:

    struct Sorter
    {
        const Partitioner & m_partitioner;
        bool operator()(const SStable & a, const SStable & b);
        bool operator()(const std::unique_ptr<SStable> & a, const std::unique_ptr<SStable> & b);
        explicit Sorter(const Partitioner & partitioner) : m_partitioner(partitioner) {}
    };

    std::vector<TableConfig>        m_tableConfig;
    const Partitioner *             m_pPartitioner;
    off_t                           m_totalFileSize;
    size_t                          m_numFiles;
    std::string                     m_keyspace;
    std::string                     m_tableName;


    static int compare_start_tokens(void * pVPartition, const void * aa, const void * bb);
};

#endif /* parse_cassandra_h */
