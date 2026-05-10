#include "altertable_storage.hpp"
#include "storage/altertable_catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/settings.hpp"
#include "storage/altertable_transaction_manager.hpp"

namespace duckdb {

static unique_ptr<Catalog> AltertableAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &attach_options) {
	auto &config = DBConfig::GetConfig(context);
	if (!Settings::Get<EnableExternalAccessSetting>(config)) {
		throw PermissionException("Attaching Altertable databases is disabled through configuration");
	}
	string attach_path = info.path;

	string secret_name;
	string schema_to_load;
	for (auto &entry : attach_options.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "secret") {
			secret_name = entry.second.ToString();
		} else if (lower_name == "schema") {
			schema_to_load = entry.second.ToString();
		} else {
			throw BinderException("Unrecognized option for Altertable attach: %s", entry.first);
		}
	}
	auto connection_string = AltertableCatalog::GetConnectionString(context, attach_path, secret_name);
	return make_uniq<AltertableCatalog>(db, std::move(connection_string), std::move(attach_path),
	                                    attach_options.access_mode, std::move(schema_to_load));
}

static unique_ptr<TransactionManager>
AltertableCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                   Catalog &catalog) {
	auto &altertable_catalog = catalog.Cast<AltertableCatalog>();
	return make_uniq<AltertableTransactionManager>(db, altertable_catalog);
}

AltertableStorageExtension::AltertableStorageExtension() {
	attach = AltertableAttach;
	create_transaction_manager = AltertableCreateTransactionManager;
}

} // namespace duckdb
