#include "storage/altertable_table_set.hpp"
#include "storage/altertable_transaction.hpp"
#include "storage/altertable_catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/altertable_schema_entry.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

AltertableTableSet::AltertableTableSet(AltertableSchemaEntry &schema, unique_ptr<AltertableResultSlice> table_result_p)
    : AltertableInSchemaSet(schema, false), table_result(std::move(table_result_p)) {
}

string AltertableTableSet::GetInitializeQuery(const string &catalog, const string &schema, const string &table) {
	string base_query = R"(
SELECT
    t.table_schema AS schema_name,
    t.table_name   AS table_name,
    c.column_name  AS attname,
    c.data_type    AS type_name,
    c.numeric_precision,
    c.numeric_scale,
    c.ordinal_position AS attnum,
    CASE WHEN c.is_nullable = 'NO' THEN true ELSE false END AS notnull
FROM information_schema.tables t
JOIN information_schema.columns c
  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
WHERE t.table_type IN ('BASE TABLE', 'VIEW') ${CONDITION}
ORDER BY t.table_schema, t.table_name, c.ordinal_position;
)";
	string condition;
	if (!catalog.empty()) {
		condition += "AND t.table_catalog = " + KeywordHelper::WriteQuoted(catalog);
		condition += " AND c.table_catalog = " + KeywordHelper::WriteQuoted(catalog);
	}
	if (!schema.empty()) {
		condition += " AND t.table_schema = " + KeywordHelper::WriteQuoted(schema);
	}
	if (!table.empty()) {
		condition += " AND t.table_name = " + KeywordHelper::WriteQuoted(table);
	}
	return StringUtil::Replace(base_query, "${CONDITION}", condition);
}

void AltertableTableSet::AddColumn(optional_ptr<AltertableTransaction> transaction,
                                   optional_ptr<AltertableSchemaEntry> schema, AltertableResult &result, idx_t row,
                                   AltertableTableInfo &table_info) {
	AltertableTypeData type_info;
	auto column_name = result.GetString(row, 2);
	type_info.type_name = result.GetString(row, 3);
	type_info.numeric_precision = (int32_t)result.GetInt64(row, 4);
	type_info.numeric_scale = (int32_t)result.GetInt64(row, 5);
	bool is_not_null = result.GetBool(row, 7);

	AltertableType altertable_type;
	auto column_type = AltertableUtils::TypeToLogicalType(transaction, schema, type_info, altertable_type);
	table_info.altertable_types.push_back(std::move(altertable_type));
	table_info.altertable_names.push_back(column_name);
	ColumnDefinition column(std::move(column_name), std::move(column_type));
	auto &create_info = *table_info.create_info;
	if (is_not_null) {
		create_info.constraints.push_back(
		    make_uniq<NotNullConstraint>(LogicalIndex(create_info.columns.PhysicalColumnCount())));
	}
	create_info.columns.AddColumn(std::move(column));
}

void AltertableTableSet::CreateEntries(AltertableTransaction &transaction, AltertableResult &result, idx_t start,
                                       idx_t end) {
	vector<unique_ptr<AltertableTableInfo>> tables;
	unique_ptr<AltertableTableInfo> info;

	for (idx_t row = start; row < end; row++) {
		auto table_name = result.GetString(row, 1);
		if (!info || info->GetTableName() != table_name) {
			if (info) {
				tables.push_back(std::move(info));
			}
			info = make_uniq<AltertableTableInfo>(schema, table_name);
		}
		AddColumn(&transaction, &schema, result, row, *info);
	}
	if (info) {
		tables.push_back(std::move(info));
	}
	for (auto &tbl_info : tables) {
		auto table_entry = make_shared_ptr<AltertableTableEntry>(catalog, schema, *tbl_info);
		CreateEntry(transaction, std::move(table_entry));
	}
}

void AltertableTableSet::LoadEntries(AltertableTransaction &transaction) {
	if (table_result) {
		CreateEntries(transaction, table_result->GetResult(), table_result->start, table_result->end);
		table_result.reset();
	} else {
		auto &altertable_catalog = catalog.Cast<AltertableCatalog>();
		auto query = GetInitializeQuery(altertable_catalog.GetRemoteCatalog(), schema.name);

		auto result = transaction.Query(query);
		auto rows = result->Count();

		CreateEntries(transaction, *result, 0, rows);
	}
}

unique_ptr<AltertableTableInfo> AltertableTableSet::GetTableInfo(AltertableTransaction &transaction,
                                                                 AltertableSchemaEntry &schema,
                                                                 const string &table_name) {
	auto &altertable_catalog = schema.ParentCatalog().Cast<AltertableCatalog>();
	auto query = AltertableTableSet::GetInitializeQuery(altertable_catalog.GetRemoteCatalog(), schema.name, table_name);
	auto result = transaction.Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		return nullptr;
	}
	auto table_info = make_uniq<AltertableTableInfo>(schema, table_name);
	for (idx_t row = 0; row < rows; row++) {
		AddColumn(&transaction, &schema, *result, row, *table_info);
	}
	return table_info;
}

unique_ptr<AltertableTableInfo> AltertableTableSet::GetTableInfo(AltertableConnection &connection,
                                                                 const string &schema_name, const string &table_name) {
	auto query = AltertableTableSet::GetInitializeQuery(connection.GetCatalog(), schema_name, table_name);
	auto result = connection.Query(query);
	auto rows = result->Count();
	if (rows == 0) {
		throw InvalidInputException("Table %s does not contain any columns.", table_name);
	}
	auto table_info = make_uniq<AltertableTableInfo>(schema_name, table_name);
	for (idx_t row = 0; row < rows; row++) {
		AddColumn(nullptr, nullptr, *result, row, *table_info);
	}
	return table_info;
}

optional_ptr<CatalogEntry> AltertableTableSet::ReloadEntry(AltertableTransaction &transaction,
                                                           const string &table_name) {
	auto table_info = GetTableInfo(transaction, schema, table_name);
	if (!table_info) {
		return nullptr;
	}
	auto table_entry = make_shared_ptr<AltertableTableEntry>(catalog, schema, *table_info);
	return CreateEntry(transaction, std::move(table_entry));
}

// FIXME - this is almost entirely copied from TableCatalogEntry::ColumnsToSQL - should be unified
string AltertableColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints) {
	std::stringstream ss;

	ss << "(";

	// find all columns that have NOT NULL specified, but are NOT primary key
	// columns
	logical_index_set_t not_null_columns;
	logical_index_set_t unique_columns;
	logical_index_set_t pk_columns;
	unordered_set<string> multi_key_pks;
	vector<string> extra_constraints;
	for (auto &constraint : constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &not_null = constraint->Cast<NotNullConstraint>();
			not_null_columns.insert(not_null.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &pk = constraint->Cast<UniqueConstraint>();
			vector<string> constraint_columns = pk.columns;
			if (pk.index.index != DConstants::INVALID_INDEX) {
				// no columns specified: single column constraint
				if (pk.is_primary_key) {
					pk_columns.insert(pk.index);
				} else {
					unique_columns.insert(pk.index);
				}
			} else {
				// multi-column constraint, this constraint needs to go at the end after
				// all columns
				if (pk.is_primary_key) {
					// multi key pk column: insert set of columns into multi_key_pks
					for (auto &col : pk.columns) {
						multi_key_pks.insert(col);
					}
				}
				string base = pk.is_primary_key ? "PRIMARY KEY(" : "UNIQUE(";
				for (idx_t i = 0; i < pk.columns.size(); i++) {
					if (i > 0) {
						base += ", ";
					}
					base += KeywordHelper::WriteQuoted(pk.columns[i], '"');
				}
				extra_constraints.push_back(base + ")");
			}
		} else if (constraint->type == ConstraintType::FOREIGN_KEY) {
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			if (fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				extra_constraints.push_back(constraint->ToString());
			}
		} else {
			extra_constraints.push_back(constraint->ToString());
		}
	}

	for (auto &column : columns.Logical()) {
		if (column.Oid() > 0) {
			ss << ", ";
		}
		ss << KeywordHelper::WriteQuoted(column.Name(), '"') << " ";
		ss << AltertableUtils::TypeToString(column.Type());
		bool not_null = not_null_columns.find(column.Logical()) != not_null_columns.end();
		bool is_single_key_pk = pk_columns.find(column.Logical()) != pk_columns.end();
		bool is_multi_key_pk = multi_key_pks.find(column.Name()) != multi_key_pks.end();
		bool is_unique = unique_columns.find(column.Logical()) != unique_columns.end();
		if (not_null && !is_single_key_pk && !is_multi_key_pk) {
			// NOT NULL but not a primary key column
			ss << " NOT NULL";
		}
		if (is_single_key_pk) {
			// single column pk: insert constraint here
			ss << " PRIMARY KEY";
		}
		if (is_unique) {
			// single column unique: insert constraint here
			ss << " UNIQUE";
		}
		if (column.Generated()) {
			ss << " GENERATED ALWAYS AS(" << column.GeneratedExpression().ToString() << ")";
		} else if (column.HasDefaultValue()) {
			ss << " DEFAULT(" << column.DefaultValue().ToString() << ")";
		}
	}
	// print any extra constraints that still need to be printed
	for (auto &extra_constraint : extra_constraints) {
		ss << ", ";
		ss << extra_constraint;
	}

	ss << ")";
	return ss.str();
}

string GetAltertableCreateTable(CreateTableInfo &info) {
	for (idx_t i = 0; i < info.columns.LogicalColumnCount(); i++) {
		auto &col = info.columns.GetColumnMutable(LogicalIndex(i));
		col.SetType(AltertableUtils::ToAltertableType(col.GetType()));
	}

	std::stringstream ss;
	ss << "CREATE TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!info.schema.empty()) {
		ss << KeywordHelper::WriteQuoted(info.schema, '"');
		ss << ".";
	}
	ss << KeywordHelper::WriteQuoted(info.table, '"');
	ss << AltertableColumnsToSQL(info.columns, info.constraints);
	ss << ";";
	return ss.str();
}

optional_ptr<CatalogEntry> AltertableTableSet::CreateTable(AltertableTransaction &transaction,
                                                           BoundCreateTableInfo &info) {
	auto create_sql = GetAltertableCreateTable(info.Base());
	transaction.ExecuteUpdate(create_sql);
	auto tbl_entry = make_shared_ptr<AltertableTableEntry>(catalog, schema, info.Base());
	return CreateEntry(transaction, std::move(tbl_entry));
}

string AltertableTableSet::GetAlterTablePrefix(const string &name, optional_ptr<CatalogEntry> entry) {
	string sql = "ALTER TABLE ";
	sql += KeywordHelper::WriteQuoted(schema.name, '"') + ".";
	sql += KeywordHelper::WriteQuoted(entry ? entry->name : name, '"');
	return sql;
}

string AltertableTableSet::GetAlterTableColumnName(const string &name, optional_ptr<CatalogEntry> entry) {
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return name;
	}
	auto &table = entry->Cast<AltertableTableEntry>();
	string column_name = name;
	auto column_index = table.GetColumnIndex(column_name, true);
	if (!column_index.IsValid()) {
		return name;
	}
	return table.altertable_names[column_index.index];
}

string AltertableTableSet::GetAlterTablePrefix(AltertableTransaction &transaction, const string &name) {
	auto entry = GetEntry(transaction, name);
	return GetAlterTablePrefix(name, entry);
}

void AltertableTableSet::AlterTable(AltertableTransaction &transaction, RenameTableInfo &info) {
	string sql = GetAlterTablePrefix(transaction, info.name);
	sql += " RENAME TO ";
	sql += KeywordHelper::WriteQuoted(info.new_table_name, '"');
	transaction.ExecuteUpdate(sql);
}

void AltertableTableSet::AlterTable(AltertableTransaction &transaction, RenameColumnInfo &info) {
	auto entry = GetEntry(transaction, info.name);
	string sql = GetAlterTablePrefix(info.name, entry);
	sql += " RENAME COLUMN  ";
	string column_name = GetAlterTableColumnName(info.old_name, entry);
	sql += KeywordHelper::WriteQuoted(column_name, '"');
	sql += " TO ";
	sql += KeywordHelper::WriteQuoted(info.new_name, '"');

	transaction.ExecuteUpdate(sql);
}

void AltertableTableSet::AlterTable(AltertableTransaction &transaction, AddColumnInfo &info) {
	string sql = GetAlterTablePrefix(transaction, info.name);
	sql += " ADD COLUMN  ";
	if (info.if_column_not_exists) {
		sql += "IF NOT EXISTS ";
	}
	sql += KeywordHelper::WriteQuoted(info.new_column.Name(), '"');
	sql += " ";
	sql += info.new_column.Type().ToString();
	transaction.ExecuteUpdate(sql);
}

void AltertableTableSet::AlterTable(AltertableTransaction &transaction, RemoveColumnInfo &info) {
	auto entry = GetEntry(transaction, info.name);
	string sql = GetAlterTablePrefix(info.name, entry);
	sql += " DROP COLUMN  ";
	if (info.if_column_exists) {
		sql += "IF EXISTS ";
	}
	string column_name = GetAlterTableColumnName(info.removed_column, entry);
	sql += KeywordHelper::WriteQuoted(column_name, '"');
	transaction.ExecuteUpdate(sql);
}

void AltertableTableSet::AlterTable(AltertableTransaction &transaction, AlterTableInfo &alter) {
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE:
		AlterTable(transaction, alter.Cast<RenameTableInfo>());
		break;
	case AlterTableType::RENAME_COLUMN:
		AlterTable(transaction, alter.Cast<RenameColumnInfo>());
		break;
	case AlterTableType::ADD_COLUMN:
		AlterTable(transaction, alter.Cast<AddColumnInfo>());
		break;
	case AlterTableType::REMOVE_COLUMN:
		AlterTable(transaction, alter.Cast<RemoveColumnInfo>());
		break;
	default:
		throw BinderException("Unsupported ALTER TABLE type - Altertable tables only "
		                      "support RENAME TABLE, RENAME COLUMN, "
		                      "ADD COLUMN and DROP COLUMN");
	}
	ClearEntries();
}

} // namespace duckdb
