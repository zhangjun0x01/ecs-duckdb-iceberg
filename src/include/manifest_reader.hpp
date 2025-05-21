#pragma once

#include "iceberg_options.hpp"
#include "iceberg_types.hpp"
#include "iceberg_manifest.hpp"

namespace duckdb {

// Manifest Reader

struct ManifestReaderInput {
public:
	ManifestReaderInput(const case_insensitive_map_t<ColumnIndex> &name_to_vec,
	                    sequence_number_t sequence_number_to_inherit = NumericLimits<sequence_number_t>::Maximum(),
	                    int32_t partition_spec_id = NumericLimits<int32_t>::Maximum(), bool skip_deleted = false);

public:
	const case_insensitive_map_t<ColumnIndex> &name_to_vec;
};

class BaseManifestReader {
public:
	BaseManifestReader(idx_t iceberg_version) : iceberg_version(iceberg_version) {
		if (iceberg_version > 2) {
			throw InvalidConfigurationException("Iceberg version > 2 is not supported yet");
		}
	}

public:
	void Initialize(unique_ptr<AvroScan> scan_p);

public:
	bool Finished() const;
	virtual void CreateNameMapping(idx_t i, LogicalType &type, const string &name) = 0;
	virtual void ValidateNameMapping() = 0;

private:
	unique_ptr<AvroScan> scan;
	idx_t offset = 0;
	bool finished = true;

protected:
	DataChunk chunk;
	case_insensitive_map_t<ColumnIndex> name_to_vec;
	const idx_t iceberg_version;
};

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(idx_t iceberg_version);

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);
	void CreateNameMapping(idx_t i, LogicalType &type, const string &name) override;
	void ValidateNameMapping() override;

private:
	idx_t ReadChunk(idx_t offset, idx_t size, vector<IcebergManifestEntry> &result);
};

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestFileReader : public BaseManifestReader {
public:
	ManifestFileReader(idx_t iceberg_version);

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);
	void CreateNameMapping(idx_t i, LogicalType &type, const string &name) override;
	void ValidateNameMapping() override;

public:
	void SetSequenceNumber(sequence_number_t sequence_number);
	void SetPartitionSpecID(int32_t partition_spec_id);

private:
	idx_t ReadChunk(idx_t offset, idx_t size, vector<IcebergManifestEntry> &result);

public:
	//! The sequence number to inherit when the condition to do so is met
	sequence_number_t sequence_number;
	//! The inherited partition spec id (from the 'manifest_file')
	int32_t partition_spec_id;
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

} // namespace duckdb
