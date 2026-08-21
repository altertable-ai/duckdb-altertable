#include "altertable_connection.hpp"
#include "altertable_result.hpp"
#include "altertable_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

#include "arrow/flight/sql/client.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/table.h"

namespace duckdb {

static bool debug_altertable_print_queries = false;

static void DebugPrintQueryFingerprint(const string &query) {
	if (debug_altertable_print_queries) {
		Printer::Print("Altertable " + AltertableUtils::QueryFingerprint(query) + "\n");
	}
}

static void SetAltertableSessionOption(arrow::flight::sql::FlightSqlClient &sql_client,
                                       arrow::flight::FlightCallOptions &call_options, const string &session_key,
                                       const string &value, const string &option_label) {
	arrow::flight::SetSessionOptionsRequest session_request;
	session_request.session_options.emplace(session_key, value);
	auto session_result = sql_client.SetSessionOptions(call_options, session_request);
	if (!session_result.ok()) {
		throw IOException("Failed to set Altertable session %s \"%s\": %s", option_label, value,
		                  session_result.status().ToString());
	}
	auto set_result = session_result.ValueOrDie();
	auto option_error = set_result.errors.find(session_key);
	if (option_error != set_result.errors.end()) {
		throw IOException("Failed to set Altertable session %s \"%s\": %s", option_label, value,
		                  arrow::flight::ToString(option_error->second.value));
	}
}

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
		SetAltertableSessionOption(*sql_client, call_options, "catalog", config.catalog, "catalog");
	}
	if (!config.compute_size.empty()) {
		SetAltertableSessionOption(*sql_client, call_options, "compute_size", config.compute_size, "compute size");
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
	DebugPrintQueryFingerprint(query);

	auto result = GetClient()->Execute(GetCallOptions(), query);
	if (!result.ok()) {
		throw IOException("Failed to execute query: " + result.status().ToString());
	}
	return std::move(result.ValueOrDie());
}

std::shared_ptr<arrow::Schema> AltertableConnection::GetExecuteSchema(const string &query) {
	DebugPrintQueryFingerprint(query);

	auto result = GetClient()->GetExecuteSchema(GetCallOptions(), query);
	arrow::ipc::DictionaryMemo memo;
	if (!result.ok()) {
		auto info = Execute(query);
		auto schema_result = info->GetSchema(&memo);
		if (!schema_result.ok()) {
			throw IOException("Failed to get query schema: " + schema_result.status().ToString());
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
	DebugPrintQueryFingerprint(query);

	auto result = GetClient()->ExecuteUpdate(GetCallOptions(), query);
	if (!result.ok()) {
		throw IOException("Failed to execute update: " + result.status().ToString());
	}
	return result.ValueOrDie();
}

std::unique_ptr<arrow::flight::FlightStreamReader>
AltertableConnection::QueryEndpointStream(const arrow::flight::FlightEndpoint &endpoint) {
	auto result = GetClient()->DoGet(GetCallOptions(), endpoint.ticket);
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

unique_ptr<AltertableResult> AltertableConnection::Query(const string &query) {
	auto info = Execute(query);

	if (info->endpoints().empty()) {
		return make_uniq<AltertableResult>();
	}

	vector<std::shared_ptr<arrow::Table>> tables;
	for (auto &endpoint : info->endpoints()) {
		auto stream = QueryEndpointStream(endpoint);
		auto table_result = stream->ToTable();
		if (!table_result.ok()) {
			throw IOException("Failed to read stream table: " + table_result.status().ToString());
		}
		tables.push_back(table_result.ValueOrDie());
	}
	if (tables.size() == 1) {
		return make_uniq<AltertableResult>(std::move(tables[0]));
	}
	auto concat_result = arrow::ConcatenateTables(tables);
	if (!concat_result.ok()) {
		throw IOException("Failed to concatenate Flight endpoint tables: " + concat_result.status().ToString());
	}
	return make_uniq<AltertableResult>(concat_result.ValueOrDie());
}

} // namespace duckdb
