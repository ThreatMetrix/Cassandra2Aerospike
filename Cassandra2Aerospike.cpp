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
//  Cassandra2Aerospike.cpp
//  Iterate through a cassandra table and write the contents to Aerospike

#include "AerospikeWriter.hpp"
#include "CassandraParser.hpp"
#include "DryRun.hpp"
#include "Utilities.hpp"

#include <assert.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <forward_list>
#include <iostream>
#include <limits>
#include <map>

static void signalHandler(int signalNumber)
{
    AerospikeWriter::terminate();
}


static void print_usage(const char * name)
{
    fprintf(stderr, "Usage: %s [<options>*]\n"
            "OPTIONS:\n"
            "    -i <cassandra directory>    Directory containing Cassandra database files (this option may be used multiple times)\n"
            "    -h <aerospike host>         Hostname/IP address of Aerospike host (this option may be used multiple times)\n"
            "        (If you need an Aerospike service port other than 3000, add \":<port number>\" to the IP address.)\n"
            "    [-t <aerospike table name>] If absent, the table name will be deduced from the cassandra directory.\n"
            "    [-n <aerospike namespace>]   If absent, the keyspace name will be deduced from the cassandra directory.\n"
            "    [-C]                        Disable checksum (default enabled)\n"
            "    [-e <number of event threads> (default 4)]\n"
            "    [-a <max asynchronous operations in flight per thread> (default 100)]\n"
            "    [-s key value to start processing]\n"
            "    [-S key value to start processing (represented in hexedecimal)\n"
            "    [-L <TTL limit in seconds>] All records with a TTL less than the given number of seconds are discarded\n"
            "    [-x]                        Prohibit Aerospike records that do not expire (they are given the Aerospike namespace's default TTL).\n"
            "    [-f]                        Use first expiring column in Cassandra to calculate TTL (default = use last)\n"
            "    [-u <user name>]            Select user name for Aerospike security credentials (default = none)\n"
            "    [-p <password>]             Select password for Aerospike security credentials (default = none)\n"
            "    [-D]                        Dry run (print rather than import)\n"
            "    [-v]                        Print version and exit.\n", name);
}


static int do_live_run(as_config & as, CassandraParser::iterator & iter, unsigned int numEventLoops,
                       const std::string & keyspace, const std::string & tableName);

static int do_transfer(aerospike & as, CassandraParser::iterator & iter, unsigned int numEventLoops,
                       const std::string & keyspace, const std::string & tableName);

static void wait_for_writers(std::vector<AerospikeWriter> & writers, pthread_mutex_t * status_lock, pthread_cond_t * check_status);

static int parse_arguments(int argc, char * argv[],
                           as_config & config, unsigned int & numEventLoops, std::vector<std::string> & paths, bool & dry_run,
                           std::string & set_name, std::string & name_space, const char *& firstKey)
{
    const char * user = NULL;
    const char * password = NULL;
    int opt;
    while ((opt = getopt(argc, argv, "i:t:n:h:Ca:e:Vs:S:L:xfu:p:D")) != -1)
    {
        switch (opt) {
            case 'i':
                paths.push_back(optarg);
                break;

            case 't':
                set_name.assign(optarg);
                break;

            case 'n':
                name_space.assign(optarg);
                break;

            case 'h':
            {
                if (const char * colon = std::strchr(optarg, ':'))
                {
                    int port = atoi(colon + 1);
                    std::string truncated(optarg, colon - optarg);
                    as_config_add_host(&config, truncated.c_str(), port);
                }
                else
                {
                    as_config_add_host(&config, optarg, 3000);
                }
            }
                break;

            case 'C':
                CompressedBuffer::enableChecksum(false);
                break;

            case 'a':
                AerospikeWriter::set_max_records_in_flight(atoi(optarg));
                break;

            case 'e':
                numEventLoops = atoi(optarg);
                break;

            case 's':
                firstKey = optarg;
                break;

            case 'S':
            {
                static std::string firstKeyBuffer;
                char * arg_ptr = optarg;
                while (*arg_ptr != 0)
                {
                    char first_nibble = *(arg_ptr++);

                    if (arg_ptr == 0)
                    {
                        fprintf(stderr, "-S argument must be an even length\n");
                        return EXIT_FAILURE;
                    }
                    char second_nibble = *(arg_ptr++);

                    uint8_t high, low;
                    if (!hex_nibble_to_nibble(high, first_nibble))
                        return EXIT_FAILURE;
                    if (!hex_nibble_to_nibble(low, second_nibble))
                        return EXIT_FAILURE;
                    firstKeyBuffer.push_back((high << 4) | low);
                }
                firstKey = firstKeyBuffer.c_str();
            }
                break;

            case 'L':
            {
                char * endPtr;
                int64_t ttlLimitSeconds = strtol(optarg, &endPtr, 10);
                if (ttlLimitSeconds < 1 || ttlLimitSeconds > std::numeric_limits<uint32_t>::max() || *endPtr != 0)
                {
                    fprintf(stderr, "Invalid ttl %s (must be number 1<=x<= %llu)\n", optarg, static_cast<long long>(std::numeric_limits<uint32_t>::max()));
                    return 1;
                }
                AerospikeWriter::set_minimum_ttl(ttlLimitSeconds);
            }
                break;

            case 'x':
                AerospikeWriter::set_prohibit_eternal_records();
                break;

            case 'f':
                AerospikeWriter::set_use_nearest_timeout();
                break;

            case 'u':
                user = optarg;
                break;

            case 'p':
                password = optarg;
                break;

            case 'D':
                dry_run = true;
                break;

            default: /* '?' */
                fprintf(stderr, "Unrecognised option %c\n", (char)opt);
                return -1;
        }
    }

    if (optind < argc)
    {
        fprintf(stderr, "ERROR: superfluous parameters:");
        for (int i = optind; i < argc; ++i)
            fprintf(stderr, " %s", argv[i]);
        fprintf(stderr, "\n");
        return -1;
    }

    if ((user == nullptr) != (password == nullptr))
    {
        fprintf(stderr, "Invalid arguments: %s\n",
                password == nullptr ? "empty password and non-empty user name" : "empty user name and non-empty password");

        return -1;
    }
    else if (user != nullptr && password != nullptr)
    {
        if (as_config_set_user(&config, user, password))
        {
            printf("Aerospike_Manager set user to %s\n", user);
        }
        else
        {
            fprintf(stderr, "as_config_set_user failed (%s)", user); // (Don't print password.)
        }
    }

    if (paths.empty())
    {
        fprintf(stderr, "Invalid arguments: paths empty\n");
        return -1;
    }

    if (!dry_run && config.hosts == nullptr)
    {
        fprintf(stderr, "Invalid arguments: no aerospike hosts specified\n");
        return -1;
    }
    return 0;
}

int main(int argc, char * argv[])
{
    unsigned int numEventLoops = 4;

    std::vector<std::string> paths;
    const char * firstKey = NULL;
    std::string set_name, name_space;
    bool dry_run = false;
    as_config config;
    as_config_init(&config);

    config.policies.write.base.socket_timeout = 0; // Retry up to 15 times, over 1.5s.
    config.policies.write.exists = AS_POLICY_EXISTS_CREATE; // Specifies the behavior for the existence of the record: Create a record, ONLY if it doesn't exist.
    config.policies.write.base.max_retries = 14; // Maximum number of retries when a transaction fails due to a network error.
    config.policies.write.base.total_timeout = 1500;

    if (parse_arguments(argc, argv, config, numEventLoops, paths, dry_run, set_name, name_space, firstKey))
    {
        print_usage(argv[0]);
        return -1;
    }

    CassandraParser parser;
    if (!parser.open(paths))
    {
        return -1;
    }

    if (parser.getNumFiles() == 0)
    {
        fprintf(stderr, "Note: there were no files to process\n");
        return 0;
    }

    if (name_space.empty())
    {
        name_space = parser.getKeyspace();
    }

    if (set_name.empty())
    {
        set_name = parser.getTableName();
    }

    if (parser.getKeyspace() != name_space || parser.getTableName() != set_name)
    {
        fprintf(stderr, "Warning: keyspace and table from database files (%s, %s) are not consistent with command line arguments (%s, %s)\n",
                parser.getKeyspace().c_str(), parser.getTableName().c_str(), name_space.c_str(), set_name.c_str());
    }

    struct sigaction signalAction;
    memset(&signalAction, 0, sizeof(signalAction));
    signalAction.sa_handler = &signalHandler;
    signalAction.sa_flags = SA_RESETHAND;
    if (sigaction(SIGTERM, &signalAction, NULL) < 0 ||
        sigaction(SIGINT, &signalAction, NULL) < 0)
    {
        fprintf(stderr, "ERROR: sigaction() failed: errno = %d\n", errno);
        return 1;
    }


    CassandraParser::iterator iter = firstKey == NULL ? parser.begin() : parser.find(firstKey);
    if (dry_run)
    {
        do_dry_run(iter);
        return 0;
    }
    else
    {
        return do_live_run(config, iter, numEventLoops, name_space, set_name);
    }
}

static int do_live_run(as_config & config, CassandraParser::iterator & iter, unsigned int numEventLoops,
                       const std::string & name_space, const std::string & set_name)
{
    as_event_loop * loops = as_event_create_loops(numEventLoops);
    // Create the event loops (separate threads) for Aerospike async operation
    if (loops == nullptr)
    {
        fprintf(stderr, "Failed to create %u event loops\n", numEventLoops);
        return -1;
    }

    assert(as_event_loop_size == numEventLoops);

    // Initialise policy structures that can be given to Aerospike during writes and reads, to control policy.

    int return_code = -1;
    if (aerospike* as = aerospike_new(&config))
    {
        return_code = do_transfer(*as, iter, numEventLoops, name_space, set_name);
        aerospike_destroy(as);
    }
    else
    {
        fprintf(stderr, "ERROR: Aerospike cluster failed when creating Aerospike object with aerospike_new\n");
    }

    as_event_close_loops();
    return return_code;
}

static int do_transfer(aerospike & as, CassandraParser::iterator & iter, unsigned int numEventLoops,
                       const std::string & keyspace, const std::string & tableName)
{
    as_error err;
    if (aerospike_connect(&as, &err) != AEROSPIKE_OK)
    {
        fprintf(stderr, "ERROR: Aerospike cluster failed connection, error(%d) %s at [%s:%d]\n", err.code, err.message, err.file, err.line);
        return -1;
    }

    pthread_mutex_t iterator_mutex;
    // Only use a mutex if there is more than one thread.
    pthread_mutex_t * iterator_mutex_ptr = numEventLoops > 1 ? &iterator_mutex : nullptr;
    pthread_mutex_t status_lock;
    pthread_cond_t check_status;

    if (pthread_mutex_init(&status_lock, nullptr) != 0 ||
        pthread_cond_init(&check_status, nullptr) != 0 ||
        (iterator_mutex_ptr != nullptr && pthread_mutex_init(iterator_mutex_ptr, nullptr) != 0))
    {
        fprintf(stderr, "ERROR: cannot init mutex %d\n", errno);
        aerospike_close(&as, &err);
        return -1;
    }

    std::vector<AerospikeWriter> writers;
    writers.reserve(numEventLoops);
    for (unsigned int i = 0; i < numEventLoops; i++)
    {
        writers.emplace_back(iter, as, keyspace.c_str(), tableName.c_str(), iterator_mutex_ptr, &status_lock, &check_status);
    }

    for (unsigned int index = 0; index < writers.size(); index++)
    {
        writers[index].write_next(as_event_loop_get_by_index(index));
    }

    wait_for_writers(writers, &status_lock, &check_status);

    size_t total_existing = 0;
    size_t total_failed = 0;
    size_t total_expired = 0;
    for (const AerospikeWriter & writer : writers)
    {
        total_existing += writer.get_existing_entries();
        total_failed += writer.get_failed_entries();
        total_expired += writer.get_expired_entries();
    }

    printf("Exported %lu records, failed to write %zu records, skipped %lu deleted/expired records, skipped %lu records that were already in Aerospike.\n",
           iter.getCassandraReadRecords() - total_existing - total_failed - total_expired, total_failed, iter.getSkippedRecords() + total_expired, total_existing);
    std::string first_unsent;
    if (AerospikeWriter::get_first_unsent_record(first_unsent, writers) ||
        iter.get_next_key(first_unsent))
    {
        bool printable = isPrintable(first_unsent);
        std::string as_hex = binaryToHex(first_unsent);
        printf("Export incomplete. Next time you may resume with by adding: %s %s\n",
               printable ? "-s" : "-S",
               printable ? first_unsent.c_str() : as_hex.c_str());
    }
    else
    {
        printf("Export complete\n");
    }

    aerospike_close(&as, &err);

    // These mutexes must be destroyed after aerospike has been shut down.
    if (iterator_mutex_ptr)
    {
        pthread_mutex_destroy(iterator_mutex_ptr);
        iterator_mutex_ptr = nullptr;
    }

    pthread_mutex_destroy(&status_lock);
    pthread_cond_destroy(&check_status);

    return 0;
}


// This will wait for the writers to terminate. If writers get into a bad state, it will pause and restart them.
static void wait_for_writers(std::vector<AerospikeWriter> & writers, pthread_mutex_t * status_lock, pthread_cond_t * check_status)
{
    while(true)
    {
        pthread_mutex_lock(status_lock);

        while (std::find_if(writers.begin(), writers.end(),
                            [](const AerospikeWriter & writer) {
                                return writer.get_status() != AerospikeWriter::RUNNING;
                            }) == writers.end())
        {
            pthread_cond_wait(check_status, status_lock);
        }

        std::vector<size_t> stalled_indexes;
        for (size_t i = 0; i < writers.size(); i++)
        {
            if (writers[i].get_status() == AerospikeWriter::STALLED)
            {
                stalled_indexes.push_back(i);
            }
        }

        pthread_mutex_unlock(status_lock);

        // If there are no stalled indexes, it follows that they are all finished.
        if (std::find_if(writers.begin(), writers.end(),
                         [](const AerospikeWriter & writer) {
                             return writer.get_status() != AerospikeWriter::FINISHED;
                         }) == writers.end())
        {
            break;
        }

        // If any writer has entered a "stalled" state (no records in flight, but records waiting to resend), it needs to be given a kick back into life.
        usleep(150000);
        for (size_t index : stalled_indexes)
        {
            writers[index].write_next(as_event_loop_get_by_index(index));
        }
    }
}
