//===----------------------------------------------------------------------===//
//                         DuckDB
//
// altertable_result_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/interval.hpp"
#include "altertable_conversion.hpp"
#include "altertable_utils.hpp"

namespace duckdb {
class AltertableConnection;
struct AltertableBindData;

enum class AltertableReadResult { FINISHED, HAVE_MORE_TUPLES };

struct AltertableResultReader {
	explicit AltertableResultReader(AltertableConnection &con_p, const vector<column_t> &column_ids,
	                              const AltertableBindData &bind_data)
	    : con(con_p), column_ids(column_ids), bind_data(bind_data) {
	}
	virtual ~AltertableResultReader() = default;

	AltertableConnection &GetConn() {
		return con;
	}

public:
	virtual void BeginCopy(const string &sql) = 0;
	virtual AltertableReadResult Read(DataChunk &result) = 0;

protected:
	AltertableConnection &con;
	const vector<column_t> &column_ids;
	const AltertableBindData &bind_data;
};

} // namespace duckdb
