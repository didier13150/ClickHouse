#include <Core/Status.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Protocol
{

void Status::Request::write(WriteBuffer & out, UInt64 server_protocol_revision) const
{
    writeVarUInt(tables.size(), out);
    for (const auto & table : tables)
        writeBinary(table, out);
}

void Status::Request::read(ReadBuffer & in, UInt64 client_protocol_revision)
{
    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Poco::Exception("Too large collection size.");

    for (size_t i = 0; i < size; ++i)
    {
        std::string table;
        readBinary(table, in);
        tables.emplace(std::move(table));
    }
}

void Status::Response::TableStatus::write(WriteBuffer & out, UInt64 client_protocol_revision) const
{
    writeBinary(is_replicated, out);
    if (is_replicated)
    {
        writeVarUInt(absolute_delay, out);
        writeVarUInt(relative_delay, out);
    }
}

void Status::Response::TableStatus::read(ReadBuffer & in, UInt64 server_protocol_revision)
{
    absolute_delay = 0;
    relative_delay = 0;
    readBinary(is_replicated, in);
    if (is_replicated)
    {
        readVarUInt(absolute_delay, in);
        readVarUInt(relative_delay, in);
    }
}

void Status::Response::write(WriteBuffer & out, UInt64 client_protocol_revision) const
{
    writeVarUInt(table_states_by_id.size(), out);
    for (const auto & kv: table_states_by_id)
    {
        const std::string & id = kv.first;
        const TableStatus & status = kv.second;
        writeBinary(id, out);
        status.write(out, client_protocol_revision);
    }
}

void Status::Response::read(ReadBuffer & in, UInt64 server_protocol_revision)
{
    size_t size = 0;
    readVarUInt(size, in);

    if (size > DEFAULT_MAX_STRING_SIZE)
        throw Poco::Exception("Too large collection size.");

    for (size_t i = 0; i < size; ++i)
    {
        std::string table;
        readBinary(table, in);
        TableStatus status;
        status.read(in, server_protocol_revision);
        table_states_by_id.emplace(std::move(table), std::move(status));
    }
}

}
}
