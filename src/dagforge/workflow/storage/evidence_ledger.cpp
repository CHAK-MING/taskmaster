#include "dagforge/workflow/evidence_ledger.hpp"

#include "detail/durable_file.hpp"
#include "detail/storage_codec.hpp"

#include <cstddef>
#include <filesystem>
#include <algorithm>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace dagforge::workflow {

auto EvidenceLedger::open(std::filesystem::path file,
                          std::size_t max_records,
                          std::size_t max_file_bytes,
                          std::size_t max_record_bytes)
    -> Result<std::shared_ptr<EvidenceLedger>> {
  if (file.empty() || max_records == 0 || max_file_bytes == 0 ||
      max_record_bytes == 0 || max_record_bytes > max_file_bytes) {
    return fail(Error::InvalidArgument);
  }
  auto ledger = std::shared_ptr<EvidenceLedger>(new EvidenceLedger(
      std::move(file), max_records, max_file_bytes, max_record_bytes));
  auto loaded = ledger->load_file();
  if (!loaded) {
    return fail(loaded.error());
  }
  return ok(std::move(ledger));
}

auto EvidenceLedger::load_file() -> Result<void> {
  if (file_.empty()) {
    return ok();
  }
  auto contents = storage_detail::load_text_file(file_, max_file_bytes_);
  if (!contents) {
    if (contents.error() == make_error_code(Error::NotFound)) {
      return ok();
    }
    return fail(contents.error());
  }

  file_bytes_ = contents->size();
  bool rewrite = false;
  std::size_t start = 0;
  while (start < contents->size()) {
    const auto separator = contents->find('\n', start);
    const bool terminated = separator != std::string::npos;
    const auto end = terminated ? separator : contents->size();
    const std::string_view line{contents->data() + start, end - start};
    if (line.size() > max_record_bytes_) {
      return fail(Error::ResourceExhausted);
    }
    if (!line.empty()) {
      auto record = storage_detail::decode_evidence(line);
      if (!record) {
        if (!terminated &&
            record.error() == make_error_code(Error::Incomplete)) {
          rewrite = true;
          break;
        }
        return fail(record.error() == make_error_code(Error::Incomplete)
                        ? Error::ParseError
                        : record.error());
      }
      records_.push_back(std::move(*record));
    }
    if (!terminated) {
      rewrite = true;
      break;
    }
    start = separator + 1;
  }
  if (records_.size() > max_records_) {
    records_.erase(records_.begin(),
                   records_.end() - static_cast<std::ptrdiff_t>(max_records_));
    rewrite = true;
  }
  if (rewrite) {
    auto rewritten = rewrite_file(records_);
    if (!rewritten) {
      return fail(rewritten.error());
    }
    if (rewritten->durability_deferred) {
      return fail(Error::PersistenceError);
    }
  }
  stale_records_ = 0;
  return ok();
}

auto EvidenceLedger::encode_line(const EvidenceRecord &record) const
    -> Result<std::string> {
  auto encoded = storage_detail::encode_evidence(record);
  if (!encoded) {
    return fail(encoded.error());
  }
  if (encoded->size() > max_record_bytes_) {
    return fail(Error::ResourceExhausted);
  }
  encoded->push_back('\n');
  return encoded;
}

auto EvidenceLedger::append_line(std::string_view line) -> Result<WriteResult> {
  if (file_.empty()) {
    return ok(WriteResult{});
  }
  auto appended = storage_detail::append_text_file_durable(
      file_, line, max_file_bytes_);
  if (!appended) {
    return fail(appended.error());
  }
  file_bytes_ += line.size();
  durability_deferred_ =
      durability_deferred_ || !appended->durability_confirmed();
  return ok(WriteResult{
      .durability_deferred = durability_deferred_,
  });
}

auto EvidenceLedger::rewrite_file(const std::vector<EvidenceRecord> &records)
    -> Result<WriteResult> {
  if (file_.empty()) {
    return ok(WriteResult{});
  }
  try {
    std::string contents;
    for (const auto &record : records) {
      auto encoded = storage_detail::encode_evidence(record);
      if (!encoded) {
        return fail(encoded.error());
      }
      if (encoded->size() > max_record_bytes_ ||
          contents.size() > max_file_bytes_ ||
          encoded->size() + 1 > max_file_bytes_ - contents.size()) {
        return fail(Error::ResourceExhausted);
      }
      contents.append(*encoded);
      contents.push_back('\n');
    }
    auto rewritten = storage_detail::store_text_file_atomic(file_, contents);
    if (!rewritten) {
      return fail(rewritten.error());
    }
    file_bytes_ = contents.size();
    stale_records_ = 0;
    durability_deferred_ = !rewritten->durability_confirmed();
    return ok(WriteResult{
        .durability_deferred = durability_deferred_,
    });
  } catch (const std::bad_alloc &) {
    return fail(Error::ResourceExhausted);
  } catch (const std::length_error &) {
    return fail(Error::ResourceExhausted);
  }
}

auto EvidenceLedger::compaction_record_threshold() const noexcept
    -> std::size_t {
  constexpr std::size_t kMinimumBatch = 64;
  constexpr std::size_t kRetentionFraction = 10;
  return std::max(kMinimumBatch, max_records_ / kRetentionFraction);
}

auto EvidenceLedger::append(EvidenceRecord record)
    -> Result<EvidenceAppendResult> {
  if (record.run_id.empty()) {
    return fail(Error::InvalidArgument);
  }
  if (record.evidence_id.empty()) {
    record.evidence_id = generate_evidence_id();
  }
  auto id = record.evidence_id;
  std::lock_guard lock(mutex_);
  if (max_records_ == 0) {
    return ok(EvidenceAppendResult{.evidence_id = std::move(id)});
  }
  if (file_.empty()) {
    if (records_.size() == max_records_) {
      records_.erase(records_.begin());
    }
    records_.push_back(std::move(record));
    return ok(EvidenceAppendResult{.evidence_id = std::move(id)});
  }
  auto line = encode_line(record);
  if (!line) {
    return fail(line.error());
  }
  if (records_.size() < max_records_) {
    auto persisted = append_line(*line);
    if (!persisted) {
      return fail(persisted.error());
    }
    records_.push_back(std::move(record));
    return ok(EvidenceAppendResult{
        .evidence_id = std::move(id),
        .durability_deferred = persisted->durability_deferred,
    });
  }

  const bool file_would_overflow =
      !file_.empty() &&
      (file_bytes_ > max_file_bytes_ ||
       line->size() > max_file_bytes_ - file_bytes_);
  const bool compaction_due =
      stale_records_ + 1 >= compaction_record_threshold();
  if (file_would_overflow || compaction_due) {
    auto compacted = records_;
    compacted.erase(compacted.begin());
    compacted.push_back(std::move(record));
    auto persisted = rewrite_file(compacted);
    if (!persisted) {
      return fail(persisted.error());
    }
    records_ = std::move(compacted);
    return ok(EvidenceAppendResult{
        .evidence_id = std::move(id),
        .durability_deferred = persisted->durability_deferred,
    });
  }

  auto persisted = append_line(*line);
  if (!persisted) {
    return fail(persisted.error());
  }
  records_.erase(records_.begin());
  records_.push_back(std::move(record));
  ++stale_records_;
  return ok(EvidenceAppendResult{
      .evidence_id = std::move(id),
      .durability_deferred = persisted->durability_deferred,
  });
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
