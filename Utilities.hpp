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
//  Utilities.hpp
//  Utility functions for importer

#ifndef Utilities_hpp
#define Utilities_hpp

#include <string>

std::string binaryToHex(const std::string& bin);
bool isPrintable(const std::string& val);
bool hex_nibble_to_nibble(uint8_t & nibble_out, const char hex_nibble_in);

#endif

