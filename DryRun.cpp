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
//  DryRun.cpp
//  Will print out records from a cassandra iterator.

#include "DryRun.hpp"
#include "AerospikeWriter.hpp"
#include "Utilities.hpp"

class TestDatabaseRow : public CassandraParser::DatabaseRow
{
public:
    virtual void new_row(const std::string & key_string) final
    {
        if (isPrintable(key_string))
        {
            printf ("%s:\n", key_string.c_str());
        }
        else
        {
            printf ("%s:\n", binaryToHex(key_string).c_str());
        }
    }

    // This are for columns with no expiry time set
    virtual void new_column(const std::string & column_name, const std::string & column_value, int64_t ts) final
    {
        if (isPrintable(column_value))
        {
            printf ("%s=%s\n", column_name.c_str(), column_value.c_str());
        }
        else
        {
            printf ("%s=%s\n", column_name.c_str(), binaryToHex(column_value).c_str());
        }
    }

    // This is for columns that do expire.
    virtual void new_column_with_ttl(const std::string & column_name, const std::string & column_value,
                                     int64_t ts, uint32_t ttl, uint32_t ttlTimestampSecs) final
    {
        if (isPrintable(column_value))
        {
            printf ("%s=%s (timeout=%lu)\n", column_name.c_str(), column_value.c_str(), (unsigned long)ttlTimestampSecs);
        }
        else
        {
            printf ("%s=%s (timeout=%lu)\n", column_name.c_str(), binaryToHex(column_value).c_str(), (unsigned long)ttlTimestampSecs);
        }
    }
};

void do_dry_run(CassandraParser::iterator & iter)
{
    TestDatabaseRow row;
    while(iter.next(row) && !AerospikeWriter::terminated())
    {
        ;
    }
}
