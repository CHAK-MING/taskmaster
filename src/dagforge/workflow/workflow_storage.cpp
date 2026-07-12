#include "dagforge/workflow/workflow_storage.hpp"

#include <openssl/evp.h>

#include <array>
#include <memory>
#include <string>
#include <utility>

namespace dagforge::workflow {
namespace {

[[nodiscard]] auto digest_bytes(std::span<const std::byte> data)
    -> Result<std::string> {
  auto context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>{
      EVP_MD_CTX_new(), &EVP_MD_CTX_free};
  if (!context || EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1 ||
      EVP_DigestUpdate(context.get(), data.data(), data.size()) != 1) {
    return fail(Error::Unknown);
  }

  std::array<unsigned char, EVP_MAX_MD_SIZE> bytes{};
  unsigned int size = 0;
  if (EVP_DigestFinal_ex(context.get(), bytes.data(), &size) != 1) {
    return fail(Error::Unknown);
  }

  static constexpr char kHex[] = "0123456789abcdef";
  std::string out(static_cast<std::size_t>(size) * 2, '\0');
  for (unsigned int i = 0; i < size; ++i) {
    out[static_cast<std::size_t>(i) * 2] = kHex[bytes[i] >> 4U];
    out[static_cast<std::size_t>(i) * 2 + 1] = kHex[bytes[i] & 0x0fU];
  }
  return ok(std::move(out));
}

} // namespace

auto InMemoryArtifactStore::put(std::span<const std::byte> data,
                                std::string media_type)
    -> Result<ArtifactRef> {
  auto digest = digest_bytes(data);
  if (!digest) {
    return fail(digest.error());
  }

  ArtifactBlob blob;
  blob.ref = ArtifactRef{
      .artifact_id = generate_artifact_id(),
      .media_type = std::move(media_type),
      .size_bytes = static_cast<std::uint64_t>(data.size()),
      .digest = std::move(*digest),
  };
  blob.data.assign(data.begin(), data.end());

  auto ref = blob.ref;
  std::lock_guard lock(mutex_);
  artifacts_.emplace(ref.artifact_id.str(), std::move(blob));
  return ok(std::move(ref));
}

auto InMemoryArtifactStore::get(const ArtifactId &artifact_id) const
    -> Result<ArtifactBlob> {
  std::lock_guard lock(mutex_);
  const auto it = artifacts_.find(artifact_id.str());
  if (it == artifacts_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto InMemoryArtifactStore::erase(const ArtifactId &artifact_id)
    -> Result<void> {
  std::lock_guard lock(mutex_);
  if (artifacts_.erase(artifact_id.str()) == 0) {
    return fail(Error::NotFound);
  }
  return ok();
}

auto InMemoryArtifactStore::size() const -> std::size_t {
  std::lock_guard lock(mutex_);
  return artifacts_.size();
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
  records_.push_back(std::move(record));
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

auto CheckpointStore::save(WorkflowCheckpoint checkpoint) -> Result<void> {
  if (checkpoint.run_id.empty() || checkpoint.plan_id.empty()) {
    return fail(Error::InvalidArgument);
  }
  std::lock_guard lock(mutex_);
  checkpoints_[checkpoint.run_id.str()] = std::move(checkpoint);
  return ok();
}

auto CheckpointStore::load(const WorkflowRunId &run_id) const
    -> Result<WorkflowCheckpoint> {
  std::lock_guard lock(mutex_);
  const auto it = checkpoints_.find(run_id.str());
  if (it == checkpoints_.end()) {
    return fail(Error::NotFound);
  }
  return ok(it->second);
}

auto CheckpointStore::erase(const WorkflowRunId &run_id) -> Result<void> {
  std::lock_guard lock(mutex_);
  if (checkpoints_.erase(run_id.str()) == 0) {
    return fail(Error::NotFound);
  }
  return ok();
}

} // namespace dagforge::workflow
