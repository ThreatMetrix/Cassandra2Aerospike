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
//  SSTableSchema.hpp
//  Processes the schema used by SSTables version ma and above.

#ifndef SSTableSchema_hpp
#define SSTableSchema_hpp

#include <string>
#include <vector>

class Buffer;

// This is used by format MA and above to signified how each column is streamed
struct TableSchema
{
    enum ColumnFormat
    {
        COLUMN_TEXT,
        COLUMN_INT32,
        COLUMN_UUID,
        COLUMN_FLOAT,
        COLUMN_LONG,
        COLUMN_BOOL,
        COLUMN_EMPTY,
        COLUMN_TIMESTAMP,
        COLUMN_COUNTER,
        COLUMN_UNKNOWN
    };

    void parse(Buffer & buf);
    static void read_columns(Buffer & buf, std::vector<std::pair<std::string, ColumnFormat>> & columns);
    static ColumnFormat read_column_format(Buffer & buf);
    static size_t get_column_size(ColumnFormat column, Buffer & buf);

    uint64_t minTimestamp;
    uint64_t minTTL;
    ColumnFormat keyType;
    std::vector<ColumnFormat> clustering;
    std::vector<std::pair<std::string, ColumnFormat>> static_columns;
    std::vector<std::pair<std::string, ColumnFormat>> regular_columns;
};

#endif /* SSTableSchema_hpp */
