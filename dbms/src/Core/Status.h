#pragma once

#include <string>
#include <unordered_set>
#include <unordered_map>

#include <Core/Types.h>
#include <Common/SipHash.h>

namespace DB
{

struct QualifiedTableName
{
    std::string database;
    std::string table;

    bool operator==(const QualifiedTableName & other) const
    {
        return database == other.database && table == other.table;
    }

    bool operator<(const QualifiedTableName & other) const
    {
        if (database == other.database)
            return table < other.table;
        return database < other.database;
    }

    UInt64 hash() const
    {
        SipHash hash_state;
        hash_state.update(database.data(), database.size());
        hash_state.update(table.data(), table.size());
        return hash_state.get64();
    }
};

}

namespace std
{

template<> struct hash<DB::QualifiedTableName>
{
    typedef DB::QualifiedTableName argument_type;
    typedef std::size_t result_type;

    result_type operator()(const argument_type & qualified_table) const
    {
        return qualified_table.hash();
    }
};

}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

namespace Protocol
{

struct Status
{
    struct Request
    {
        std::unordered_set<QualifiedTableName> tables;

        void write(WriteBuffer & out, UInt64 server_protocol_revision) const;
        void read(ReadBuffer & in, UInt64 client_protocol_revision);
    };

    struct Response
    {
        struct TableStatus
        {
            bool is_replicated;
            UInt32 absolute_delay;
            UInt32 relative_delay;

            void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
            void read(ReadBuffer & in, UInt64 server_protocol_revision);
        };

        std::unordered_map<QualifiedTableName, TableStatus> table_states_by_id;

        void write(WriteBuffer & out, UInt64 client_protocol_revision) const;
        void read(ReadBuffer & in, UInt64 server_protocol_revision);
    };
};

}

}
