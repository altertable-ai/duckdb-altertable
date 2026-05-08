#include "altertable_connection.hpp"
#include "altertable_result.hpp"
#include "altertable_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

#include "arrow/flight/sql/client.h"
#include "arrow/ipc/dictionary.h"

namespace duckdb {

static bool debug_altertable_print_queries = false;

AltertableConnection::AltertableConnection(shared_ptr<OwnedAltertableConnection> connection_p)
    : connection(std::move(connection_p)) {
}

AltertableConnection::~AltertableConnection() {
	Close();
}

AltertableConnection::AltertableConnection(AltertableConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

AltertableConnection &AltertableConnection::operator=(AltertableConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

AltertableConnection AltertableConnection::Open(const string &dsn) {
	auto config = AltertableConnectionConfig::Parse(dsn);

	arrow::Result<arrow::flight::Location> location_result;

	if (config.ssl) {
		location_result = arrow::flight::Location::ForGrpcTls(config.host, config.port);
	} else {
		location_result = arrow::flight::Location::ForGrpcTcp(config.host, config.port);
	}

	if (!location_result.ok()) {
		throw IOException("Failed to create location: " + location_result.status().ToString());
	}

	auto location = location_result.ValueOrDie();

	auto flight_client_result = arrow::flight::FlightClient::Connect(location);

	if (!flight_client_result.ok()) {
		throw IOException("Failed to connect to Flight server: " + flight_client_result.status().ToString());
	}

	auto flight_client = std::move(flight_client_result).ValueOrDie();

	// Authentication setup
	arrow::flight::FlightCallOptions call_options;
	if (!config.user.empty() && !config.password.empty()) {
		auto auth_result = flight_client->AuthenticateBasicToken({}, config.user, config.password);
		if (!auth_result.ok()) {
			throw IOException("Authentication failed: " + auth_result.status().ToString());
		}

		auto bearer_token = auth_result.ValueOrDie();
		call_options.headers.push_back(bearer_token);
	}

	auto sql_client = std::make_unique<arrow::flight::sql::FlightSqlClient>(std::move(flight_client));

	if (!config.catalog.empty()) {
		arrow::flight::SetSessionOptionsRequest session_request;
		session_request.session_options.emplace("catalog", config.catalog);
		auto session_result = sql_client->SetSessionOptions(call_options, session_request);
		if (!session_result.ok()) {
			throw IOException("Failed to set Altertable session catalog \"%s\": %s", config.catalog,
			                  session_result.status().ToString());
		}
		auto set_result = session_result.ValueOrDie();
		auto catalog_error = set_result.errors.find("catalog");
		if (catalog_error != set_result.errors.end()) {
			throw IOException("Failed to set Altertable session catalog \"%s\": %s", config.catalog,
			                  arrow::flight::ToString(catalog_error->second.value));
		}
	}

	auto connection = make_shared_ptr<OwnedAltertableConnection>(std::move(sql_client));
	connection->call_options = call_options;
	connection->catalog = config.catalog;

	AltertableConnection result;
	result.connection = std::move(connection);
	result.dsn = dsn;
	return result;
}

std::unique_ptr<arrow::flight::FlightInfo> AltertableConnection::Execute(const string &query) {
	if (DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	auto result = GetClient()->Execute(GetCallOptions(), query);
	if (!result.ok()) {
		throw IOException("Failed to execute query: " + result.status().ToString());
	}
	return std::move(result.ValueOrDie());
}

std::shared_ptr<arrow::Schema> AltertableConnection::GetExecuteSchema(const string &query) {
	if (DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	auto result = GetClient()->GetExecuteSchema(GetCallOptions(), query);
	arrow::ipc::DictionaryMemo memo;
	if (!result.ok()) {
		auto info = Execute(query);
		auto schema_result = info->GetSchema(&memo);
		if (!schema_result.ok()) {
			throw IOException("Failed to get query schema: " + result.status().ToString());
		}
		return schema_result.ValueOrDie();
	}
	auto schema_result = result.ValueOrDie()->GetSchema(&memo);
	if (!schema_result.ok()) {
		throw IOException("Failed to deserialize query schema: " + schema_result.status().ToString());
	}
	return schema_result.ValueOrDie();
}

int64_t AltertableConnection::ExecuteUpdate(const string &query) {
	if (DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}

	auto result = GetClient()->ExecuteUpdate(GetCallOptions(), query);
	if (!result.ok()) {
		throw IOException("Failed to execute update: " + result.status().ToString());
	}
	return result.ValueOrDie();
}

std::unique_ptr<arrow::flight::FlightStreamReader> AltertableConnection::QueryStream(const string &query) {
	auto info = Execute(query);

	// For now we just read the first endpoint
	if (info->endpoints().empty()) {
		throw IOException("No endpoints returned for query");
	}

	auto result = GetClient()->DoGet(GetCallOptions(), info->endpoints()[0].ticket);
	if (!result.ok()) {
		throw IOException("Failed to get stream: " + result.status().ToString());
	}

	return std::move(result.ValueOrDie());
}

bool AltertableConnection::IsOpen() {
	return connection.get() != nullptr;
}

void AltertableConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

void AltertableConnection::DebugSetPrintQueries(bool print) {
	debug_altertable_print_queries = print;
}

bool AltertableConnection::DebugPrintQueries() {
	return debug_altertable_print_queries;
}

vector<unique_ptr<AltertableResult>> AltertableConnection::ExecuteQueries(const string &queries) {
	// For now, just execute as a single query
	Execute(queries);
	vector<unique_ptr<AltertableResult>> results;
	results.push_back(make_uniq<AltertableResult>());
	return results;
}

unique_ptr<AltertableResult> AltertableConnection::Query(const string &query) {
	auto info = Execute(query);

	// For DDL/DML statements and SELECT queries, we need to fetch the results
	if (info->endpoints().empty()) {
		return make_uniq<AltertableResult>();
	}

	auto stream_result = GetClient()->DoGet(GetCallOptions(), info->endpoints()[0].ticket);
	if (!stream_result.ok()) {
		throw IOException("Failed to get stream: " + stream_result.status().ToString());
	}
	auto stream = std::move(stream_result.ValueOrDie());

	// Read all batches into a table
	auto table_result = stream->ToTable();
	if (!table_result.ok()) {
		throw IOException("Failed to read stream table: " + table_result.status().ToString());
	}
	return make_uniq<AltertableResult>(table_result.ValueOrDie());
}

} // namespace duckdb
