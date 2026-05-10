//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/altertable_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
struct DropInfo;
class AltertableResult;
class AltertableSchemaEntry;
class AltertableTransaction;

class AltertableCatalogSet {
public:
	virtual ~AltertableCatalogSet() = default;
	AltertableCatalogSet(Catalog &catalog, bool is_loaded);

	optional_ptr<CatalogEntry> GetEntry(AltertableTransaction &transaction, const string &name);
	void DropEntry(AltertableTransaction &transaction, DropInfo &info);
	void Scan(AltertableTransaction &transaction, const std::function<void(CatalogEntry &)> &callback);
	virtual optional_ptr<CatalogEntry> CreateEntry(AltertableTransaction &transaction, shared_ptr<CatalogEntry> entry);
	void ClearEntries();
	virtual bool SupportReload() const {
		return false;
	}
	virtual optional_ptr<CatalogEntry> ReloadEntry(AltertableTransaction &transaction, const string &name);

protected:
	virtual void LoadEntries(AltertableTransaction &transaction) = 0;
	//! Whether or not the catalog set contains dependencies to itself that have
	//! to be resolved WHILE loading
	virtual bool HasInternalDependencies() const {
		return false;
	}
	void TryLoadEntries(AltertableTransaction &transaction);

protected:
	Catalog &catalog;

private:
	mutex entry_lock;
	mutex load_lock;
	unordered_map<string, shared_ptr<CatalogEntry>> entries;
	case_insensitive_map_t<string> entry_map;
	atomic<bool> is_loaded;
};

class AltertableInSchemaSet : public AltertableCatalogSet {
public:
	~AltertableInSchemaSet() override = default;
	AltertableInSchemaSet(AltertableSchemaEntry &schema, bool is_loaded);

	optional_ptr<CatalogEntry> CreateEntry(AltertableTransaction &transaction, shared_ptr<CatalogEntry> entry) override;

protected:
	AltertableSchemaEntry &schema;
};

struct AltertableResultSlice {
	AltertableResultSlice(shared_ptr<AltertableResult> result_p, idx_t start, idx_t end)
	    : result(std::move(result_p)), start(start), end(end) {
	}

	AltertableResult &GetResult() {
		return *result;
	}

	shared_ptr<AltertableResult> result;
	idx_t start;
	idx_t end;
};

} // namespace duckdb
