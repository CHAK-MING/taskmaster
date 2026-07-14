#include "dagforge/workflow/evidence_ledger.hpp"

#include "detail/storage_codec.hpp"

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>
#include <utility>

namespace dagforge::workflow {

EvidenceLedger::EvidenceLedger(std::filesystem::path file,
                               std::size_t max_records)
    : file_(std::move(file)), max_records_(max_records) {
  load_file();
}

auto EvidenceLedger::load_file() -> void {
  if (file_.empty()) {
    return;
  }
  std::ifstream input(file_);
  std::string line;
  while (std::getline(input, line)) {
    if (line.empty()) {
      continue;
    }
    auto record = storage_detail::decode_evidence(line);
    if (record) {
      records_.push_back(std::move(*record));
    }
  }
  if (records_.size() > max_records_) {
    records_.erase(records_.begin(),
                   records_.end() - static_cast<std::ptrdiff_t>(max_records_));
    (void)rewrite_file();
  }
}

auto EvidenceLedger::append_file(const EvidenceRecord &record) -> Result<void> {
  if (file_.empty()) {
    return ok();
  }
  auto encoded = storage_detail::encode_evidence(record);
  if (!encoded) {
    return fail(encoded.error());
  }
  std::error_code error;
  std::filesystem::create_directories(file_.parent_path(), error);
  if (error) {
    return fail(error);
  }
  std::ofstream output(file_, std::ios::binary | std::ios::app);
  if (!output) {
    return fail(Error::Unknown);
  }
  output << *encoded << '\n';
  output.flush();
  return output ? ok() : fail(Error::Unknown);
}

auto EvidenceLedger::rewrite_file() -> Result<void> {
  if (file_.empty()) {
    return ok();
  }
  std::string contents;
  for (const auto &record : records_) {
    auto encoded = storage_detail::encode_evidence(record);
    if (!encoded) {
      return fail(encoded.error());
    }
    contents.append(*encoded);
    contents.push_back('\n');
  }
  return storage_detail::store_text_file_atomic(file_, contents);
}

auto EvidenceLedger::append(EvidenceRecord record) -> Result<EvidenceId> {
  if (record.run_id.empty()) {
    return fail(Error::InvalidArgument);
  }
  if (record.evidence_id.empty()) {
    record.evidence_id = generate_evidence_id();
  }
  auto id = record.evidence_id;
  std::lock_guard lock(mutex_);
  if (records_.size() < max_records_) {
    auto persisted = append_file(record);
    if (!persisted) {
      return fail(persisted.error());
    }
    records_.push_back(std::move(record));
    return ok(std::move(id));
  }

  auto previous = records_;
  records_.push_back(std::move(record));
  records_.erase(records_.begin());
  auto persisted = rewrite_file();
  if (!persisted) {
    records_ = std::move(previous);
    return fail(persisted.error());
  }
  return ok(std::move(id));
}

auto EvidenceLedger::records(const WorkflowRunId &run_id) const
    -> std::vector<EvidenceRecord> {
  std::vector<EvidenceRecord> out;
  std::lock_guard lock(mutex_);
  for (const auto &record : records_) {
    if (record.run_id == run_id) {
      out.push_back(record);
    }
  }
  return out;
}

auto EvidenceLedger::size() const -> std::size_t {
  std::lock_guard lock(mutex_);
  return records_.size();
}

} // namespace dagforge::workflow
