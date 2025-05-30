//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/multi_file/multi_file_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/extra_operator_info.hpp"
#include "duckdb/common/open_file_info.hpp"

namespace duckdb {
class MultiFileList;

enum class FileExpandResult : uint8_t { NO_FILES, SINGLE_FILE, MULTIPLE_FILES };

struct MultiFileListScanData {
	idx_t current_file_idx = DConstants::INVALID_INDEX;
};

class MultiFileListIterationHelper {
public:
	DUCKDB_API explicit MultiFileListIterationHelper(MultiFileList &collection);

private:
	MultiFileList &file_list;

private:
	class MultiFileListIterator;

	class MultiFileListIterator {
	public:
		DUCKDB_API explicit MultiFileListIterator(MultiFileList *file_list);

		optional_ptr<MultiFileList> file_list;
		MultiFileListScanData file_scan_data;
		OpenFileInfo current_file;

	public:
		DUCKDB_API void Next();

		DUCKDB_API MultiFileListIterator &operator++();
		DUCKDB_API bool operator!=(const MultiFileListIterator &other) const;
		DUCKDB_API const OpenFileInfo &operator*() const;
	};

public:
	MultiFileListIterator begin(); // NOLINT: match stl API
	MultiFileListIterator end();   // NOLINT: match stl API
};

struct MultiFilePushdownInfo {
	explicit MultiFilePushdownInfo(LogicalGet &get);
	MultiFilePushdownInfo(idx_t table_index, const vector<string> &column_names, const vector<column_t> &column_ids,
	                      ExtraOperatorInfo &extra_info);

	idx_t table_index;
	const vector<string> &column_names;
	vector<column_t> column_ids;
	vector<ColumnIndex> column_indexes;
	ExtraOperatorInfo &extra_info;
};

//! Abstract class for lazily generated list of file paths/globs
//! NOTE: subclasses are responsible for ensuring thread-safety
class MultiFileList {
public:
	explicit MultiFileList(vector<OpenFileInfo> paths, FileGlobOptions options);
	virtual ~MultiFileList();

	//! Returns the raw, unexpanded paths, pre-filter
	const vector<OpenFileInfo> GetPaths() const;

	//! Get Iterator over the files for pretty for loops
	MultiFileListIterationHelper Files();

	//! Initialize a sequential scan over a file list
	void InitializeScan(MultiFileListScanData &iterator);
	//! Scan the next file into result_file, returns false when out of files
	bool Scan(MultiFileListScanData &iterator, OpenFileInfo &result_file);

	//! Returns the first file or an empty string if GetTotalFileCount() == 0
	OpenFileInfo GetFirstFile();
	//! Syntactic sugar for GetExpandResult() == FileExpandResult::NO_FILES
	bool IsEmpty();

	//! Virtual functions for subclasses
public:
	virtual unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                        MultiFilePushdownInfo &info,
	                                                        vector<unique_ptr<Expression>> &filters);
	virtual unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                        const vector<string> &names,
	                                                        const vector<LogicalType> &types,
	                                                        const vector<column_t> &column_ids,
	                                                        TableFilterSet &filters) const;

	virtual vector<OpenFileInfo> GetAllFiles() = 0;
	virtual FileExpandResult GetExpandResult() = 0;
	virtual idx_t GetTotalFileCount() = 0;

	virtual unique_ptr<NodeStatistics> GetCardinality(ClientContext &context);
	virtual unique_ptr<MultiFileList> Copy();

protected:
	//! Get the i-th expanded file
	virtual OpenFileInfo GetFile(idx_t i) = 0;

protected:
	//! The unexpanded input paths
	const vector<OpenFileInfo> paths;
	//! Whether paths can expand to 0 files
	const FileGlobOptions glob_options;

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

//! MultiFileList that takes a list of files and produces the same list of paths. Useful for quickly wrapping
//! existing vectors of paths in a MultiFileList without changing any code
class SimpleMultiFileList : public MultiFileList {
public:
	//! Construct a SimpleMultiFileList from a list of already expanded files
	explicit SimpleMultiFileList(vector<OpenFileInfo> paths);
	//! Copy `paths` to `filtered_files` and apply the filters
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                const vector<string> &names, const vector<LogicalType> &types,
	                                                const vector<column_t> &column_ids,
	                                                TableFilterSet &filters) const override;

	//! Main MultiFileList API
	vector<OpenFileInfo> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;

protected:
	//! Main MultiFileList API
	OpenFileInfo GetFile(idx_t i) override;
};

//! MultiFileList that takes a list of paths and produces a list of files with all globs expanded
class GlobMultiFileList : public MultiFileList {
public:
	GlobMultiFileList(ClientContext &context, vector<OpenFileInfo> paths, FileGlobOptions options);
	//! Calls ExpandAll, then prunes the expanded_files using the hive/filename filters
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) override;
	unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                const vector<string> &names, const vector<LogicalType> &types,
	                                                const vector<column_t> &column_ids,
	                                                TableFilterSet &filters) const override;

	//! Main MultiFileList API
	vector<OpenFileInfo> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;

protected:
	//! Main MultiFileList API
	OpenFileInfo GetFile(idx_t i) override;

	//! Get the i-th expanded file
	OpenFileInfo GetFileInternal(idx_t i);
	//! Grabs the next path and expands it into Expanded paths: returns false if no more files to expand
	bool ExpandNextPath();
	//! Grabs the next path and expands it into Expanded paths: returns false if no more files to expand
	bool ExpandPathInternal(idx_t &current_path, vector<OpenFileInfo> &result) const;
	//! Whether all files have been expanded
	bool IsFullyExpanded() const;

	//! The ClientContext for globbing
	ClientContext &context;
	//! The current path to expand
	idx_t current_path;
	//! The expanded files
	vector<OpenFileInfo> expanded_files;

	mutable mutex lock;
};

} // namespace duckdb
