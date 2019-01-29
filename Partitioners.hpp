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
//  Partitioners.hpp
//  Implements the various partitioners used by Cassandra.

#ifndef Partitioners_hpp
#define Partitioners_hpp

#include "CassandraParser.hpp"

class Partitioner
{
public:
    virtual void assign_token(CassandraParser::Token & token, const char * key, size_t key_length) const = 0;
    virtual int compare_token(const CassandraParser::Token & tokenA, const std::string & keyA, const CassandraParser::Token & tokenB, const std::string & keyB) const = 0;
    virtual ~Partitioner() {}
    
    static const Partitioner * partitioner_from_name(const char * partitionerIdentifier);
    static const Partitioner * random_partitioner();
};

#endif /* Partitioners_hpp */
