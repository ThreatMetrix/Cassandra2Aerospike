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
//  AerospikeWriter.hpp
//  Multithreaded writer to send cassandra records to aerospike.

#ifndef AerospikeWriter_hpp
#define AerospikeWriter_hpp

#include "CassandraParser.hpp"

extern "C"
{
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_event.h>
#include <aerospike/as_record.h>
}


class DatabaseRowWithWriter;

// This is a class that represents one thread's worth of Aerospike context.
// Each event thread has one of these to keep it full of data.
class AerospikeWriter
{
    aerospike & as;
    as_namespace aero_namespace;
    as_set aero_set;
    size_t requests_in_flight;
    CassandraParser::iterator & iterator;
    DatabaseRowWithWriter * failed_requests;
    DatabaseRowWithWriter * spare_requests;
    pthread_mutex_t *iterator_mutex;
    pthread_mutex_t *status_lock;
    pthread_cond_t *check_status;
    size_t existing_entries;
    size_t failed_entries;
    size_t expired_entries;
    static bool s_terminated;

public:
    enum WriterStatus
    {
        RUNNING,
        FINISHED,
        STALLED
    };
    WriterStatus writerStatus;

    AerospikeWriter(CassandraParser::iterator & it, aerospike & connection, const char * ns, const char * set, pthread_mutex_t *m, pthread_mutex_t *sl, pthread_cond_t *cs) :
    as(connection),
    requests_in_flight(0),
    iterator(it),
    failed_requests(nullptr),
    spare_requests(nullptr),
    iterator_mutex(m),
    status_lock(sl),
    check_status(cs),
    existing_entries(0),
    failed_entries(0),
    expired_entries(0),
    writerStatus(STALLED)
    {
        strncpy(aero_namespace, ns, sizeof(aero_namespace));
        strncpy(aero_set, set, sizeof(aero_set));
    }

    ~AerospikeWriter();

    DatabaseRowWithWriter * get_failed_request();
    DatabaseRowWithWriter * make_row();
    void return_row_to_pool(DatabaseRowWithWriter * row);
    void queue_row_for_resend(DatabaseRowWithWriter * row);
    void set_status_if_no_queries_in_flight(WriterStatus new_status);

    bool write_next(as_event_loop* event_loop);

    WriterStatus get_status() const
    {
        return writerStatus;
    }

    size_t get_existing_entries() const
    {
        return existing_entries;
    }

    void increment_existing_entries()
    {
        existing_entries++;
    }

    size_t get_failed_entries() const
    {
        return failed_entries;
    }

    void increment_failed_entries()
    {
        failed_entries++;
    }

    size_t get_requests_in_flight() const
    {
        return requests_in_flight;
    }

    void increment_expired_entries()
    {
        expired_entries++;
    }

    size_t get_expired_entries() const
    {
        return expired_entries;
    }

    static void set_prohibit_eternal_records();
    static void set_minimum_ttl(uint32_t ttl);
    static void set_max_records_in_flight(size_t n_records);
    static void set_use_nearest_timeout();
    static void terminate();
    static bool terminated()
    {
        return s_terminated;
    }
    static bool get_first_unsent_record(std::string & string, const std::vector<AerospikeWriter> & writers);
};


#endif /* AerospikeWriter_hpp */
