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
//  SSTableSchema.cpp
//  Processes the schema used by SSTables version ma and above.

#include "SSTableSchema.hpp"
#include "Buffer.hpp"

#include <cstring>

TableSchema::ColumnFormat TableSchema::read_column_format(Buffer & buf)
{
    const std::string identifier = buf.read_vint_length_string();

    const char * name = identifier.c_str();
    constexpr char class_prefix[] = "org.apache.cassandra.db.marshal.";
    constexpr size_t class_prefix_len = sizeof(class_prefix) - 1;
    if (strncmp(name, class_prefix, class_prefix_len) != 0)
    {
        return COLUMN_UNKNOWN;
    }

    const char * class_name = name + class_prefix_len;
    if (std::strcmp(class_name, "UTF8Type") == 0 || std::strcmp(class_name, "AsciiType") == 0)
    {
        return COLUMN_TEXT;
    }
    if (std::strcmp(class_name, "LongType") == 0)
    {
        return COLUMN_LONG;
    }
    if (std::strcmp(class_name, "Int32Type") == 0)
    {
        return COLUMN_INT32;
    }
    if (std::strcmp(class_name, "BoolType") == 0)
    {
        return COLUMN_BOOL;
    }
    if (std::strcmp(class_name, "FloatType") == 0)
    {
        return COLUMN_FLOAT;
    }
    if (std::strcmp(class_name, "FloatType") == 0)
    {
        return COLUMN_FLOAT;
    }
    if (std::strcmp(class_name, "EmptyType") == 0)
    {
        return COLUMN_FLOAT;
    }
    if (std::strcmp(class_name, "TimestampType") == 0)
    {
        return COLUMN_TIMESTAMP;
    }
    if (std::strcmp(class_name, "UUIDType") == 0 || std::strcmp(class_name, "TimeUUIDType") == 0 || std::strcmp(class_name, "LexicalUUIDType") == 0)
    {
        return COLUMN_UUID;
    }
    return COLUMN_UNKNOWN;
}

size_t TableSchema::get_column_size(size_t column, Buffer & buf) const
{
    TableSchema::ColumnFormat format = regular_columns[column].second;
    switch(format)
    {
        case TableSchema::COLUMN_TEXT:
        case TableSchema::COLUMN_UNKNOWN:
            return buf.read_unsigned_vint();

        case TableSchema::COLUMN_INT32:
            return 4;

        case TableSchema::COLUMN_UUID:
            return 16;

        case TableSchema::COLUMN_FLOAT:
            return 4;

        case TableSchema::COLUMN_LONG:
            return 8;

        case TableSchema::COLUMN_BOOL:
            return 1;

        case TableSchema::COLUMN_EMPTY:
            return 0;

        case TableSchema::COLUMN_TIMESTAMP:
            return 8;
    }
}

void TableSchema::read_columns(Buffer & buf, std::vector<std::pair<std::string, TableSchema::ColumnFormat>> & columns)
{
    uint64_t nColumns = buf.read_unsigned_vint();
    for (uint64_t i = 0; i < nColumns; i++)
    {
        std::string name = buf.read_vint_length_string();
        columns.push_back(std::make_pair(name, read_column_format(buf)));
    }
}

void TableSchema::parse(Buffer & buf)
{
    minTimestamp = buf.read_unsigned_vint();
    buf.read_unsigned_vint(); // minLocalDeletionTime
    minTTL = buf.read_unsigned_vint();

    keyType = read_column_format(buf);
    uint64_t nClusteringTypes = buf.read_unsigned_vint();
    for (uint64_t i = 0; i < nClusteringTypes; i++)
    {
        clustering.push_back(read_column_format(buf));
    }

    read_columns(buf, static_columns);
    read_columns(buf, regular_columns);
}
