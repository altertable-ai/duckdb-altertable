#include "altertable_connection.hpp"
#include "altertable_result.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

#include "arrow/flight/sql/client.h"

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

// Simple DSN parser - looks for key=value pairs
static std::unordered_map<string, string> ParseDSN(const string &dsn) {
	std::unordered_map<string, string> config;
	auto params = StringUtil::Split(dsn, " ");
	for (const auto &param : params) {
		if (param.empty())
			continue;
		auto kv = StringUtil::Split(param, "=");
		if (kv.size() == 2) {
			config[StringUtil::Lower(kv[0])] = kv[1];
		}
	}
	return config;
}

AltertableConnection AltertableConnection::Open(const string &dsn) {
	auto config = ParseDSN(dsn);

	string host = "flight.altertable.ai";
	int port = 443;
	string user = "";
	string password = "";
	string catalog = "";
	bool use_tls = true;

	if (config.find("host") != config.end())
		host = config["host"];
	if (config.find("port") != config.end()) {
		try {
			port = std::stoi(config["port"]);
		} catch (const std::exception &) {
			throw InvalidInputException("Invalid ALTERTABLE port value '%s'", config["port"]);
		}
	}
	if (config.find("user") != config.end())
		user = config["user"];
	if (config.find("password") != config.end())
		password = config["password"];
	if (config.find("ssl") != config.end())
		use_tls = config["ssl"] != "false";
	if (config.find("dbname") != config.end())
		catalog = config["dbname"];

	arrow::Result<arrow::flight::Location> location_result;

	if (use_tls) {
		location_result = arrow::flight::Location::ForGrpcTls(host, port);
	} else {
		location_result = arrow::flight::Location::ForGrpcTcp(host, port);
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
	if (!user.empty() && !password.empty()) {
		auto auth_result = flight_client->AuthenticateBasicToken({}, user, password);
		if (!auth_result.ok()) {
			throw IOException("Authentication failed: " + auth_result.status().ToString());
		}

		auto bearer_token = auth_result.ValueOrDie();
		call_options.headers.push_back(bearer_token);
	}

	auto sql_client = std::make_unique<arrow::flight::sql::FlightSqlClient>(std::move(flight_client));

	auto connection = make_shared_ptr<OwnedAltertableConnection>(std::move(sql_client));
	connection->call_options = call_options;
	connection->catalog = catalog;

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
