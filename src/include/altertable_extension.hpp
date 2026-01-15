#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb/main/extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

class AltertableExtension : public Extension {
public:
	std::string Name() override {
		return "altertable";
	}
	void Load(ExtensionLoader &loader) override;
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(altertable, loader);
}
