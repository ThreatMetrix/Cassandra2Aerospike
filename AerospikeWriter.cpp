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
//  AerospikeWriter.cpp
//  Multithreaded writer to send cassandra records to aerospike.

#include "AerospikeWriter.hpp"
#include "CassandraParser.hpp"
#include "Utilities.hpp"

#include <limits>
#include <list>

static size_t s_max_requests_in_flight = 100;
static bool s_use_nearest_timeout = false;
// This may be set to AS_RECORD_NO_EXPIRE_TTL (if records are allowed not to expire) or AS_RECORD_DEFAULT_TTL (if they are not)
static uint32_t s_ttl_for_eternal_records = AS_RECORD_NO_EXPIRE_TTL;
static uint32_t s_minimum_ttl = 1;

// Implements virtual functions in CassandraParser::DatabaseRow to receive and process row info
class AerospikeDatabaseRow : public CassandraParser::DatabaseRow
{
public:
    AerospikeDatabaseRow()
    {
        reset();
    }

    void reset()
    {
        key.clear();
        columns.clear();
        if (s_use_nearest_timeout)
        {
            expiry = std::numeric_limits<uint32_t>::max();
        }
        else
        {
            expiry = std::numeric_limits<uint32_t>::min();
        }
    }

    virtual void new_row(const std::string & key_string) final
    {
        key = key_string;
    }

    // This are for columns with no expiry time set
    virtual void new_column(const std::string & column_name, const std::string & column_value, int64_t ts) final
    {
        if (!s_use_nearest_timeout)
        {
            expiry = std::numeric_limits<uint32_t>::max();
        }
        columns.push_back(std::make_pair(column_name, column_value));
    }

    // This is for columns that do expire.
    virtual void new_column_with_ttl(const std::string & column_name, const std::string & column_value,
                                     int64_t ts, uint32_t ttl, uint32_t ttlTimestampSecs) final
    {
        if ((ttlTimestampSecs < expiry) == s_use_nearest_timeout)
        {
            expiry = ttlTimestampSecs;
        }
        columns.push_back(std::make_pair(column_name, column_value));
    }

    std::string key;
    std::vector<std::pair<std::string, std::string>> columns;
    uint32_t expiry;
};

// This is an object with a pointer to the AerospikeWriter as well as the ability to work
// as a linked-list. It may be owned by Aerospike (when it is live), or AerospikeWriter.
class DatabaseRowWithWriter final : public AerospikeDatabaseRow
{
public:
    DatabaseRowWithWriter(AerospikeWriter * w) :
        writer(w),
        next_node(nullptr)
    {
    }

    void link(DatabaseRowWithWriter *& first_link)
    {
        next_node = first_link;
        first_link = this;
    }

    void unlink(DatabaseRowWithWriter *& first_link)
    {
        first_link = next_node;
        next_node = nullptr;
    }

    static void free_row_list(DatabaseRowWithWriter *& list)
    {
        while (list != nullptr)
        {
            DatabaseRowWithWriter * previous = list;
            list = list->next_node;
            delete previous;
        }
    }

    bool handle_error_and_retry(as_error* err);
    static void write_listener(as_error* err, void* udata, as_event_loop* event_loop);
    static void pipeline_listener(void* udata, as_event_loop* event_loop);
    enum WriteReturnValue
    {
        WRITE_SUCCESS,
        WRITE_FAIL,
        WRITE_ALREADY_EXPIRED
    };
    WriteReturnValue write(as_event_loop* event_loop, aerospike & connection, const as_namespace & ns, const as_set & set,
               as_error & err);
    DatabaseRowWithWriter * next() { return next_node; }

    uint64_t ordinal;
private:
    AerospikeWriter * writer;
    DatabaseRowWithWriter * next_node;
};

// Returns true if error is transient or false if not.
bool DatabaseRowWithWriter::handle_error_and_retry(as_error* err)
{
    if (err)
    {
        // If a record already exists, then it is not an error.
        // If a record is busy, it must already exist
        if (err->code == AEROSPIKE_ERR_RECORD_EXISTS ||
            err->code == AEROSPIKE_ERR_RECORD_BUSY)
        {
            writer->increment_existing_entries();
            return false;
        }

        switch (err->code)
        {
            case AEROSPIKE_ERR_TIMEOUT:
            case AEROSPIKE_ERR_ASYNC_QUEUE_FULL:
            case AEROSPIKE_ERR_CONNECTION:
            case AEROSPIKE_ERR_NO_MORE_CONNECTIONS:
            case AEROSPIKE_ERR_ASYNC_CONNECTION:
            case AEROSPIKE_ERR_CLUSTER:
                printf("aerospike_key_put_async() returned %d - %s (retrying)\n", err->code, err->message);
                return true;
            default:
                break;
        }

        std::string as_hex;
        const char * print_this;
        if (isPrintable(key))
        {
            print_this = key.c_str();
        }
        else
        {
            as_hex = binaryToHex(key);
            print_this = as_hex.c_str();
        }
        printf("aerospike_key_put_async() returned %d - %s (key:\"%s\" failed)\n", err->code, err->message, print_this);
    }

    return false;
}

// connection has finished sending a message
void DatabaseRowWithWriter::write_listener(as_error* err, void* udata, as_event_loop* event_loop)
{
    DatabaseRowWithWriter* row = static_cast<DatabaseRowWithWriter*>(udata);
    AerospikeWriter * context = row->writer;

    if (row->handle_error_and_retry(err))
    {
        context->queue_row_for_resend(row);
    }
    else
    {
        context->return_row_to_pool(row);
        context->write_next(event_loop);
    }

}

// connection is ready to send another message.
void DatabaseRowWithWriter::pipeline_listener(void* udata, as_event_loop* event_loop)
{
    DatabaseRowWithWriter* row = static_cast<DatabaseRowWithWriter*>(udata);
    AerospikeWriter * context = row->writer;

    // Check if pipeline has space.
    // If so, send another request
    if (context->get_requests_in_flight() < s_max_requests_in_flight)
    {
        context->write_next(event_loop);
    }
}


// Write whatever is in this row to the database.
DatabaseRowWithWriter::WriteReturnValue DatabaseRowWithWriter::write(as_event_loop* event_loop, aerospike & connection, const as_namespace & ns, const as_set & set, as_error & err)
{
    as_key aerospike_key;
    as_key_init_rawp(&aerospike_key, ns, set,
                     reinterpret_cast<const uint8_t *>(key.data()), key.size(), false);

    as_record rec;
    as_record_inita(&rec, columns.size());

    for (const auto & column : columns)
    {
        as_record_set_raw(&rec, column.first.c_str(),
                          reinterpret_cast<const uint8_t *>(column.second.data()),
                          column.second.length());
    }

    if (expiry == std::numeric_limits<uint32_t>::max())
    {
        rec.ttl = s_ttl_for_eternal_records;
    }
    else
    {
        time_t now = time(NULL);
        if (expiry >= now + s_minimum_ttl)
        {
            rec.ttl = expiry - now;
        }
        else
        {
            as_record_destroy(&rec);
            as_key_destroy(&aerospike_key);
            return WRITE_ALREADY_EXPIRED;
        }
    }

    as_status status = aerospike_key_put_async(&connection, &err, NULL, &aerospike_key, &rec, write_listener, this, event_loop, pipeline_listener);

    as_record_destroy(&rec);
    as_key_destroy(&aerospike_key);
    return status == AEROSPIKE_OK ? WRITE_SUCCESS : WRITE_FAIL;
}

bool AerospikeWriter::s_terminated = false;

AerospikeWriter::~AerospikeWriter()
{
    DatabaseRowWithWriter::free_row_list(failed_requests);
    DatabaseRowWithWriter::free_row_list(spare_requests);
}

// Return the first failed request in the list for retry.
DatabaseRowWithWriter * AerospikeWriter::get_failed_request()
{
    if (failed_requests == nullptr)
    {
        return nullptr;
    }

    // If there is is a failed request that needs to be retried, do that first.
    DatabaseRowWithWriter* row = failed_requests;
    row->unlink(failed_requests);
    requests_in_flight++;
    return row;
}

// Create a new row object
DatabaseRowWithWriter * AerospikeWriter::make_row()
{
    requests_in_flight++;
    if (spare_requests)
    {
        // Take a spare request off the pool if there is one.
        DatabaseRowWithWriter * row = spare_requests;
        row->unlink(spare_requests);
        return row;
    }
    else
    {
        // Otherwise, make a new one.
        return new DatabaseRowWithWriter(this);
    }
}

// When a row is no longer used, it can be put in this pool.
void AerospikeWriter::return_row_to_pool(DatabaseRowWithWriter * row)
{
    requests_in_flight--;

    row->reset();
    row->link(spare_requests);
}

// When a row failed to send, put it in a list to send later
void AerospikeWriter::queue_row_for_resend(DatabaseRowWithWriter * row)
{
    requests_in_flight--;

    row->link(failed_requests);

    // A transient error has occured but this writer may not be expecting any more callbacks.
    // Instead of pausing here and locking the event thread, this tells the main thread to wake us up soon.
    set_status_if_no_queries_in_flight(STALLED);
}

// Write the next "thing" to Aerospike, be that a row it has tried before or a new row from the cassandra parser.
bool AerospikeWriter::write_next(as_event_loop* event_loop)
{
try_another_row:
    if (terminated())
    {
        set_status_if_no_queries_in_flight(FINISHED);
        return false;
    }

    DatabaseRowWithWriter* row = get_failed_request();
    if (row == nullptr)
    {
        row = make_row();
        if (iterator_mutex)
        {
            pthread_mutex_lock(iterator_mutex);
        }

        row->ordinal = iterator.getCassandraReadRecords();
        bool no_more_records = !iterator.next(*row);

        if (iterator_mutex)
        {
            pthread_mutex_unlock(iterator_mutex);
        }

        if (no_more_records)
        {
            return_row_to_pool(row);
            set_status_if_no_queries_in_flight(FINISHED);
            return false;
        }
    }

    // This will either be a no-op, or done by the main thread. Either way, it's safe.
    writerStatus = RUNNING;

    as_error err;
    switch(row->write(event_loop, as, aero_namespace, aero_set, err))
    {
        case DatabaseRowWithWriter::WRITE_SUCCESS:
            return true;

        case DatabaseRowWithWriter::WRITE_FAIL:
            if (row->handle_error_and_retry(&err))
            {
                queue_row_for_resend(row);
                return false;
            }
            break;

        case DatabaseRowWithWriter::WRITE_ALREADY_EXPIRED:
            increment_expired_entries();
            break;
    }

    return_row_to_pool(row);
    // Explicitly return to the top (instead of tail recursion) to prevent possible stack overflow.
    goto try_another_row;
}

// When there are queries in flight, status should always be RUNNING. Otherwise it might be FINISHED or STALLED
void AerospikeWriter::set_status_if_no_queries_in_flight(WriterStatus new_status)
{
    if (requests_in_flight == 0)
    {
        pthread_mutex_lock(status_lock);

        writerStatus = new_status;

        // Wake up the main thread
        pthread_cond_signal(check_status);

        pthread_mutex_unlock(status_lock);
    }
}

void AerospikeWriter::set_prohibit_eternal_records()
{
    s_ttl_for_eternal_records = AS_RECORD_DEFAULT_TTL;
}

void AerospikeWriter::set_minimum_ttl(uint32_t ttl)
{
    s_minimum_ttl = ttl;
}

void AerospikeWriter::set_max_records_in_flight(size_t n_records)
{
    s_max_requests_in_flight = n_records;
}

void AerospikeWriter::terminate()
{
    s_terminated = true;
}

void AerospikeWriter::set_use_nearest_timeout()
{
    s_use_nearest_timeout = true;
}

// Find the oldest record that is in the list of failed records waiting for resend.
bool AerospikeWriter::get_first_unsent_record(std::string & string, const std::vector<AerospikeWriter> & writers)
{
    DatabaseRowWithWriter * best_row = nullptr;
    for (const AerospikeWriter & writer : writers)
    {
        DatabaseRowWithWriter * next_row = writer.failed_requests;
        while(next_row)
        {
            if (best_row == nullptr || next_row->ordinal < best_row->ordinal)
            {
                best_row = next_row;
            }
            next_row = next_row->next();
        }
    }

    if (best_row)
    {
        string = best_row->key;
        return true;
    }
    return false;
}
