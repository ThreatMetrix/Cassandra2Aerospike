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
//  DryRun.hpp
//  Will print out records from a cassandra iterator.

#ifndef DryRun_hpp
#define DryRun_hpp

#include "CassandraParser.hpp"

void do_dry_run(CassandraParser::iterator & iter);

#endif /* DryRun_hpp */
