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
//  Utilities.cpp
//  Utility functions for importer

#include "Utilities.hpp"

std::string binaryToHex(const std::string& bin)
{
    std::string hex;
    hex.reserve(bin.size() * 2);
    for (size_t i = 0; i < bin.size(); ++i)
    {
        char thing[100];
        snprintf(thing, sizeof(thing), "%02x", bin[i] & 0xff);
        hex += thing;
    }
    return hex;
}

bool isPrintable(const std::string& val)
{
    for (size_t i = 0; i < val.size(); ++i)
        if (val[i] < ' ' || val[i] >= 0x7f)
            return false;
    return true;
}

bool hex_nibble_to_nibble(uint8_t & nibble_out, const char hex_nibble_in)
{
    if (hex_nibble_in >= '0' && hex_nibble_in <= '9')
    {
        nibble_out = hex_nibble_in - '0';
        return true;
    }
    if (hex_nibble_in >= 'A' && hex_nibble_in <= 'F')
    {
        nibble_out = hex_nibble_in - 'A' + 0xA;
        return true;
    }
    if (hex_nibble_in >= 'a' && hex_nibble_in <= 'f')
    {
        nibble_out = hex_nibble_in - 'a' + 0xa;
        return true;
    }
    fprintf(stderr, "\'%c\' is not a valid hex character\n", hex_nibble_in);
    return false;
}
