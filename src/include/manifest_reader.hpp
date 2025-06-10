#pragma once

#include "iceberg_options.hpp"

#include "metadata/iceberg_manifest.hpp"
#include "metadata/iceberg_manifest_list.hpp"

#include "avro_scan.hpp"

namespace duckdb {

// Manifest Reader

class BaseManifestReader {
public:
	BaseManifestReader(idx_t iceberg_version) : iceberg_version(iceberg_version) {
		if (iceberg_version > 2) {
			throw InvalidConfigurationException("Iceberg version > 2 is not supported yet");
		}
	}
	virtual ~BaseManifestReader() {
	}

public:
	void Initialize(unique_ptr<AvroScan> scan_p);
	bool Finished() const;
	virtual void CreateNameMapping(idx_t i, const LogicalType &type, const string &name) = 0;
	virtual bool ValidateNameMapping() = 0;

protected:
	idx_t ScanInternal(idx_t remaining);

protected:
	DataChunk chunk;
	case_insensitive_map_t<ColumnIndex> name_to_vec;
	const idx_t iceberg_version;
	unique_ptr<AvroScan> scan;
	idx_t offset = 0;
	bool finished = true;
};

//! Produces IcebergManifests read, from the 'manifest_list'
class ManifestListReader : public BaseManifestReader {
public:
	ManifestListReader(idx_t iceberg_version);
	~ManifestListReader() override {
	}

public:
	idx_t Read(idx_t count, vector<IcebergManifest> &result);
	void CreateNameMapping(idx_t i, const LogicalType &type, const string &name) override;
	bool ValidateNameMapping() override;

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifest> &result);
};

//! Produces IcebergManifestEntries read, from the 'manifest_file'
class ManifestFileReader : public BaseManifestReader {
public:
	ManifestFileReader(idx_t iceberg_version, bool skip_deleted = true);
	~ManifestFileReader() override {
	}

public:
	idx_t Read(idx_t count, vector<IcebergManifestEntry> &result);
	void CreateNameMapping(idx_t i, const LogicalType &type, const string &name) override;
	bool ValidateNameMapping() override;

public:
	void SetSequenceNumber(sequence_number_t sequence_number);
	void SetPartitionSpecID(int32_t partition_spec_id);

private:
	idx_t ReadChunk(idx_t offset, idx_t count, vector<IcebergManifestEntry> &result);

public:
	//! The sequence number to inherit when the condition to do so is met
	sequence_number_t sequence_number;
	//! The inherited partition spec id (from the 'manifest_file')
	int32_t partition_spec_id;
	//! Whether the deleted entries should be skipped outright
	bool skip_deleted = false;
};

} // namespace duckdb
