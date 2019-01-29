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
//  CassandraParser.cpp
//  An iterator class to perform a for-each loop on an on-disk cassandra table

#include "CassandraParser.hpp"
#include "Buffer.hpp"
#include "Partitioners.hpp"
#include "SSTable.hpp"

#include <algorithm>
#include <climits>
#include <cstring>
#include <iostream>
#include <map>

#include <assert.h>
#include <dirent.h>
#include <stdint.h>
#include <sys/stat.h>

const char DATA_SUFFIX[] = "-Data.db";
const char STATISTICS_SUFFIX[] = "-Statistics.db";
const size_t DATA_SUFFIX_LEN = sizeof(DATA_SUFFIX) - 1;

bool CassandraParser::Sorter::operator()(const SStable & a, const SStable & b)
{
    return m_partitioner.compare_token(a.next_token(), a.next_key(), b.next_token(), b.next_key()) < 0;
}

bool CassandraParser::Sorter::operator()(const std::unique_ptr<SStable> & a, const std::unique_ptr<SStable> & b)
{
    return m_partitioner.compare_token(a->next_token(), a->next_key(), b->next_token(), b->next_key()) < 0;
}

// Adds a filename to the set of files associated with this parser.
bool CassandraParser::add_data_file(const std::string & dir_string, const char * name, size_t namelen)
{
    const int version = SStable::getVersionFromFilename(name);
    if (version < 0)
    {
        fprintf(stderr, "Tables file name %s does not seem to have a version number in the right place\n", name);
        return false;
    }

    std::string thisKeyspace;
    std::string thisTable;
    if (!SStable::extractKeyspaceAndTable(version, name, dir_string, thisKeyspace, thisTable))
    {
        fprintf(stderr, "extractKeyspaceAndTable('%s') failed\n", name);

        return false;
    }

    if (m_keyspace.empty() && m_tableName.empty())
    {
        m_keyspace = thisKeyspace;
        m_tableName = thisTable;
    }
    else if (m_keyspace != thisKeyspace || m_tableName != thisTable)
    {
        fprintf(stderr, "ERROR: incompatible keyspace and table for '%s': %s,%s != %s,%s\n",
                name, m_keyspace.c_str(), m_tableName.c_str(), thisKeyspace.c_str(), thisTable.c_str());
        return false;
    }

    m_tableConfig.emplace_back(dir_string + std::string(name, namelen - DATA_SUFFIX_LEN), version);
    return true;
}

bool CassandraParser::open(const std::vector<std::string> & paths)
{
    m_pPartitioner = nullptr;

    const Partitioner * partitioner = nullptr;
    for (auto iter = paths.begin(); iter != paths.end(); iter++)
    {
        std::string dir_string;
        if (char * resolved = realpath(iter->c_str(), nullptr))
        {
            dir_string = resolved;
            free(resolved);
        }
        else
        {
            fprintf(stderr, "Cannot resolve directory %s: %s\n", iter->c_str(), strerror(errno));
            return false;
        }

        if (dir_string[dir_string.size() - 1u] != '/')
            dir_string.push_back('/');

        DIR * db_dir = opendir(dir_string.c_str());
        if (db_dir == NULL)
            return false;

        struct dirent * dp;
        while((dp = readdir(db_dir)) != NULL)
        {
            size_t namelen = strlen(dp->d_name);
            if(namelen > DATA_SUFFIX_LEN &&
               strcmp(dp->d_name + namelen - DATA_SUFFIX_LEN, DATA_SUFFIX) == 0)
            {
                struct stat statBuffer;
                const std::string fileName = dir_string + dp->d_name;
                if (stat(fileName.c_str(), &statBuffer) != 0)
                {
                    fprintf(stderr, "stat(\"%s\") failed: %s\n", fileName.c_str(), strerror(errno));
                    closedir(db_dir);
                    return false;
                }

                if ((statBuffer.st_mode & S_IFREG) == 0)
                {
                    continue;
                }

                m_totalFileSize += statBuffer.st_size;
                ++m_numFiles;

                if (!add_data_file(dir_string, dp->d_name, namelen))
                {
                    closedir(db_dir);
                    return false;
                }

                const TableConfig & config = m_tableConfig.back();
                UncompressedBuffer statsBuffer((config.path + STATISTICS_SUFFIX).c_str());
                if (statsBuffer.good())
                {
                    const Partitioner * thisPartitioner = SStable::read_metadata(statsBuffer, config.version, m_tableConfig.back().schema);
                    if (partitioner == nullptr)
                    {
                        partitioner = thisPartitioner;
                    }
                    else if (partitioner != thisPartitioner)
                    {
                        fprintf(stderr, "Tables do not use the same partitioner, cannot merge\n");
                        closedir(db_dir);
                        return false;
                    }
                }
            }
        }
        closedir(db_dir);
    }

    if (m_numFiles == 0)
    {
        fprintf(stderr, "No db files found in cassandra files directory.\n");
        return false;
    }

    if (partitioner == nullptr)
    {
        fprintf(stderr, "No partitioner specified\n");
        return false;
    }

    m_pPartitioner = partitioner;

    return true;
}

CassandraParser::iterator CassandraParser::begin() const
{
    std::vector<std::unique_ptr<SStable>> tables;
    int i = 0;
    for (auto iter = m_tableConfig.begin(); iter != m_tableConfig.end(); ++iter, i++)
    {
        std::unique_ptr<SStable> table(SStable::create_table(*iter));

        if (table->init(*m_pPartitioner))
        {
            tables.emplace_back(std::move(table));
        }
    }

    std::sort(tables.begin(), tables.end(), Sorter(*m_pPartitioner));

    return iterator(*this, std::move(tables));
}

CassandraParser::iterator CassandraParser::find(const std::string & primaryKey) const
{
    CassandraParser::Token first_token;
    m_pPartitioner->assign_token(first_token, primaryKey.data(), primaryKey.length());

    std::vector<std::unique_ptr<SStable>> tables;
    int i = 0;
    for (auto iter = m_tableConfig.begin(); iter != m_tableConfig.end(); ++iter, i++)
    {
        std::unique_ptr<SStable> table(SStable::create_table(*iter));

        if (table->init_at_key(*m_pPartitioner, first_token, primaryKey))
        {
            tables.emplace_back(std::move(table));
        }
    }

    std::sort(tables.begin(), tables.end(), Sorter(*m_pPartitioner));

    return iterator(*this, std::move(tables));
}

// Find set of tables with lowest ordered partition (row) key.
bool CassandraParser::iterator::match_table(size_t * matches, size_t & n_matches, size_t index)
{
    const CassandraParser::Token & next_token = m_tables[index]->next_token();
    const std::string & next_key = m_tables[index]->next_key();

    int comparison = -1;
    if (n_matches > 0)
    {
        const SStable & smallest_table = *m_tables[matches[0]];
        comparison = m_parser.m_pPartitioner->compare_token(next_token, next_key, smallest_table.next_token(), smallest_table.next_key());
    }

    if (comparison < 0)
    {
        matches[0] = index;
        n_matches = 1;
        return true;
    }
    else if (comparison == 0)
    {
        matches[n_matches++] = index;
        return true;
    }
    return false;
}

// This will make the specified table "active", i.e. spanning the position currently being iterated.
// when it is active, it will be read to see if it contains useful information
void CassandraParser::iterator::activate_table(size_t index)
{
    const std::string path = m_parser.m_tableConfig[index].path;
    if (m_tables[index] != nullptr &&
        m_tables[index]->open() &&
        !m_tables[index]->read_row(m_parser.m_pPartitioner))
    {
        m_active_tables.insert(index);
    }
}

// This will make the specified table "inactive", i.e. not spanning the position currently being iterated.
void CassandraParser::iterator::deactivate_table(size_t index)
{
    m_tables[index]->close();
    m_active_tables.erase(index);
}

size_t CassandraParser::iterator::find_first_row_matches(size_t * matches)
{
    size_t n_matches = 0;
    for (std::set<size_t>::iterator iter = m_active_tables.begin(); iter != m_active_tables.end(); iter++)
    {
        match_table(matches, n_matches, *iter);
    }

    // Find if we should open any more tables!
    while(m_next_table < m_tables.size() && match_table(matches, n_matches, m_next_table))
    {
        activate_table(m_next_table++);
    }

    assert(n_matches > 0 || m_active_tables.empty());
    return n_matches;
}


// This will find the lexical first column out of all the next columns of all the tables reading the active row.
// it will write the indexes into matched_columns and return the total number of matches to that column.
// if all the tables pointing to this row are exhausted, it will return 0
size_t CassandraParser::iterator::find_first_column_matches(size_t * matched_columns,
                                                      const size_t * matches,
                                                      const size_t n_matches)
{
    size_t column_matches = 0;
    const std::string * pMinString = NULL;
    // Columns are sorted, so find the first column
    for (size_t i = 0; i < n_matches; i++)
    {
        const size_t this_column = matches[i];
        const std::string & this_identifier = m_tables[this_column]->next_column().name;

        const int comparision = pMinString == NULL ? -1 : (std::strcmp(this_identifier.c_str(), pMinString->c_str()));

        if (comparision < 0)
        {
            pMinString = &this_identifier;
            matched_columns[0] = this_column;
            column_matches = 1;
        }
        else if (comparision == 0)
        {
            matched_columns[column_matches++] = this_column;
        }
    }
    return column_matches;
}

// This will pick the most recent out of all versions of the same column being iterated.
SStable & CassandraParser::iterator::choose_latest_match(const size_t * matched_columns, const size_t column_matches)
{
    // Find the newest value of that column
    size_t lastTs_index = matched_columns[0];
    int64_t lastTs = m_tables[lastTs_index]->next_column().ts;
    for (size_t i = 1; i < column_matches; i++)
    {
        size_t this_index = matched_columns[i];
        const int64_t thisTs = m_tables[this_index]->next_column().ts;
        if (thisTs > lastTs)
        {
            lastTs = thisTs;
            lastTs_index = this_index;
        }
    }

    return *m_tables[lastTs_index];
}

// This will add range tombstones from the SSTable into the set of tombstones being currently considered
// as well as removing any tombstones that have been passed completely.
void CassandraParser::iterator::update_tombstones(std::map<std::string, int64_t> &tombstones,
                                                   int64_t & minTime, const size_t * matches,
                                                   const size_t n_matches,
                                                   const int64_t marked_for_deletion,
                                                   const std::string & name) const
{
    for (size_t i = 0; i < n_matches; i++)
    {
        const size_t this_column = matches[i];
        if (m_tables[this_column]->next_column().range_tombstone)
        {
            const int64_t ts = m_tables[this_column]->next_column().ts;
            const std::string & range_end = m_tables[this_column]->next_column().data;
            auto found = tombstones.find(range_end);
            if (found == tombstones.end() || found->second < ts)
                tombstones[range_end] = ts;
            if (minTime == SStable::STILL_ACTIVE || minTime < ts)
                    minTime = ts;
        }
    }

    // Erase any range tombstones that we passed
    auto lower_bound = tombstones.lower_bound(name);
    if (lower_bound != tombstones.begin())
    {
        tombstones.erase(tombstones.begin(), lower_bound);
        // Recalculate the minimum timestamp of active records based on new range of tombstones
        minTime = marked_for_deletion;
        for (auto iter = tombstones.begin(); iter != tombstones.end(); iter++)
        {
            const int64_t ts = iter->second;
            if (minTime == SStable::STILL_ACTIVE || minTime < ts)
                minTime = ts;
        }
    }
}


bool CassandraParser::iterator::next(DatabaseRow & row)
{
    do
    {
        if (m_active_tables.empty())
        {
            if (m_next_table >= m_tables.size())
                return false;

            activate_table(m_next_table++);
        }
    }
    while(!next_record(row));

    return true;
}

// Get the next key to be traversed.
// Note that this MAY possibly not correspond to a the row returned by next_record as it may not be live.
bool CassandraParser::iterator::get_next_key(std::string & key)
{
    if (m_active_tables.empty() && m_next_table >= m_tables.size())
    {
        return false;
    }

    size_t * matches = (size_t *)alloca(sizeof(size_t) * m_tables.size());
    size_t n_matches = find_first_row_matches(matches);
    if (n_matches == 0)
    {
        return false;
    }
    key = m_tables[matches[0]]->next_key();
    return true;
}

// Find and construct the next whole rows of columns.
// Returns true if row is valid, false if row has already been deleted
bool CassandraParser::iterator::next_record(DatabaseRow & row)
{
    size_t * matches = (size_t *)alloca(sizeof(size_t) * m_tables.size());
    size_t n_matches = find_first_row_matches(matches);

    if (n_matches == 0)
    {
        return false;
    }

#ifdef DEBUG
    assert(m_last_key.empty() || m_parser.m_pPartitioner->compare_token(m_tables[matches[0]]->next_token(), m_tables[matches[0]]->next_key(), m_last_token, m_last_key) >= 0);
    memcpy(&m_last_token, &m_tables[matches[0]]->next_token(), sizeof(Token));
    m_last_key = m_tables[matches[0]]->next_key();

    for (size_t i = 1; i < n_matches; i++)
    {
        assert(m_tables[matches[i]]->next_key() == m_key);
    }
#endif

    row.new_row(m_tables[matches[0]]->next_key());

    // Prepare the row stuff
    int64_t marked_for_deletion = SStable::STILL_ACTIVE;
    for (size_t i = 0; i < n_matches; i++)
    {
        int64_t this_deletion = m_tables[matches[i]]->marked_for_deletion();
        if (this_deletion != SStable::STILL_ACTIVE && (marked_for_deletion == SStable::STILL_ACTIVE ||
                                                       marked_for_deletion < this_deletion))
        {
            marked_for_deletion = this_deletion;
        }
    }

    bool has_columns = false;

    std::map<std::string, int64_t> tombstones;
    int64_t minTime = marked_for_deletion;

    size_t * matched_columns = (size_t *)alloca(sizeof(size_t) * n_matches);
    while (size_t column_matches = find_first_column_matches(matched_columns, matches, n_matches))
    {
        const std::string & name = m_tables[matched_columns[0]]->next_column().name;
        update_tombstones(tombstones, minTime, matches, n_matches, marked_for_deletion, name);

        // Pick the latest
        SStable & lastest_table = choose_latest_match(matched_columns, column_matches);
        const ColumnInfo & nextColumn = lastest_table.next_column();

        // Empty names are associated with clustering columns, check to see if the row has been deleted since the value was written
        if (!name.empty() && !nextColumn.deleted && (minTime == SStable::STILL_ACTIVE || minTime < nextColumn.ts))
        {
            std::string data;
            lastest_table.read_column_data(data);
            if (nextColumn.expiring)
            {
                row.new_column_with_ttl(name, data, nextColumn.ts, nextColumn.extra_data.expiration.ttl, nextColumn.extra_data.expiration.expiration);
            }
            else
            {
                row.new_column(name, data, nextColumn.ts);
            }
            has_columns = true;
        }

        // For each matched column, move on.
        for (size_t i = 0; i < column_matches; i++)
        {
            const size_t index = matched_columns[i];
            if (!m_tables[index]->read_column())
            {
                // No more columns, remove from set of row matches (this is a no-op when removing the last table)
                size_t * position = std::find(matches, matches + n_matches, index);
                *position = matches[--n_matches];

                // And prepare the next row so its pointing in the right place
                if (m_tables[index]->read_row(m_parser.m_pPartitioner))
                {
                    // EOF
                    deactivate_table(index);
                }
            }
        }
    }
    ++m_cassandraReadRecords;

    // If an entry has been deleted and has not been written to since, don't bother returning it.
    if (marked_for_deletion != SStable::STILL_ACTIVE && !has_columns)
    {
        ++m_skippedRecords;
        return false;
    }

    return true;
}

CassandraParser::iterator::iterator(const CassandraParser & parser, std::vector<std::unique_ptr<SStable>> && tables) :
    m_parser(parser),
    m_next_table(0),
    m_skippedRecords(0),
    m_cassandraReadRecords(0)
{
    m_tables.swap(tables);
}

CassandraParser::iterator::iterator(const iterator & other) :
    m_parser(other.m_parser),
    m_next_table(other.m_next_table),
    m_active_tables(other.m_active_tables),
    m_skippedRecords(other.m_skippedRecords),
    m_cassandraReadRecords(other.m_cassandraReadRecords)
{
#ifdef DEBUG
    m_last_key = other.m_last_key;
    memcpy(m_last_token, other.m_last_token, sizeof(Token));
#endif
    for (auto & entry : other.m_tables)
    {
        m_tables.emplace_back(entry->duplicate());
    }
}

// this is required to isolate the destruction of SSTables to where they are defined
CassandraParser::iterator::~iterator()
{
}
