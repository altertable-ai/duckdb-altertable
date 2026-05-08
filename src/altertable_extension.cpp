#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "altertable_scanner.hpp"
#include "altertable_storage.hpp"
#include "altertable_extension.hpp"
#include "altertable_optimizer.hpp"
#include "storage/altertable_connection_pool.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/database_manager.hpp"

using namespace duckdb;

static void SetAltertableDebugQueryPrint(ClientContext &context, SetScope scope, Value &parameter) {
	AltertableConnection::DebugSetPrintQueries(BooleanValue::Get(parameter));
}

unique_ptr<BaseSecret> CreateAltertableSecretFunction(ClientContext &context, CreateSecretInput &input) {
	// apply any overridden settings
	vector<string> prefix_paths;
	auto result = make_uniq<KeyValueSecret>(prefix_paths, "altertable", "config", input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);

		if (lower_name == "host") {
			result->secret_map["host"] = named_param.second.ToString();
		} else if (lower_name == "user") {
			result->secret_map["user"] = named_param.second.ToString();
		} else if (lower_name == "catalog") {
			result->secret_map["catalog"] = named_param.second.ToString();
		} else if (lower_name == "database") {
			result->secret_map["catalog"] = named_param.second.ToString();
		} else if (lower_name == "dbname") {
			result->secret_map["catalog"] = named_param.second.ToString();
		} else if (lower_name == "password") {
			result->secret_map["password"] = named_param.second.ToString();
		} else if (lower_name == "port") {
			result->secret_map["port"] = named_param.second.ToString();
		} else if (lower_name == "ssl") {
			result->secret_map["ssl"] = named_param.second.ToString();
		} else {
			throw BinderException("Unknown named parameter passed to CREATE SECRET (TYPE altertable): " + lower_name);
		}
	}

	//! Set redact keys
	result->redact_keys = {"password"};
	return std::move(result);
}

void SetAltertableSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["host"] = LogicalType::VARCHAR;
	function.named_parameters["port"] = LogicalType::VARCHAR;
	function.named_parameters["password"] = LogicalType::VARCHAR;
	function.named_parameters["user"] = LogicalType::VARCHAR;
	function.named_parameters["catalog"] = LogicalType::VARCHAR;
	function.named_parameters["database"] = LogicalType::VARCHAR; // legacy alias for catalog
	function.named_parameters["dbname"] = LogicalType::VARCHAR;
	function.named_parameters["ssl"] = LogicalType::VARCHAR;
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register scan functions for direct table access via ATTACH
	AltertableScanFunction altertable_fun;
	loader.RegisterFunction(altertable_fun);

	AltertableScanFunctionFilterPushdown altertable_fun_filter_pushdown;
	loader.RegisterFunction(altertable_fun_filter_pushdown);

	// Register query function for arbitrary SQL forwarding
	AltertableQueryFunction query_func;
	loader.RegisterFunction(query_func);

	// Register execute function for DDL/DML statements
	AltertableExecuteFunction execute_func;
	loader.RegisterFunction(execute_func);

	// Register secret type for credentials
	SecretType secret_type;
	secret_type.name = "altertable";
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	loader.RegisterSecretType(secret_type);

	CreateSecretFunction altertable_secret_function = {"altertable", "config", CreateAltertableSecretFunction};
	SetAltertableSecretParameters(altertable_secret_function);
	loader.RegisterFunction(altertable_secret_function);

	// Register storage extension for ATTACH support
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["altertable"] = make_uniq<AltertableStorageExtension>();

	// Register optimizer extension for LIMIT pushdown
	config.optimizer_extensions.push_back(CreateAltertableLimitPushdownOptimizer());

	config.AddExtensionOption("altertable_debug_show_queries",
	                          "DEBUG SETTING: print all queries sent to Altertable to stdout", LogicalType::BOOLEAN,
	                          Value::BOOLEAN(false), SetAltertableDebugQueryPrint);

	config.AddExtensionOption("altertable_connection_cache",
	                          "Whether to reuse pooled Arrow Flight SQL connections between DuckDB transactions",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true),
	                          AltertableConnectionPool::AltertableSetConnectionCache);
}

void AltertableExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(altertable, loader) {
	LoadInternal(loader);
}
}
