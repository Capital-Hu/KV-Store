#include "kvstore/manifest.h"
#include "common/coding.h"

#include <set>
#include <stdexcept>

namespace kvstore {
namespace {

void Validate(const Manifest& manifest) {
    if (manifest.sstable_ids.size() > kMaxManifestTables) {
        throw std::length_error("Too many SSTables in manifest");
    }
    if (manifest.next_file_id == 0 || manifest.wal_id >= manifest.next_file_id) {
        throw std::invalid_argument("Invalid manifest WAL or next file ID");
    }
    std::set<std::uint64_t> seen;
    if (manifest.frozen_wal_id && *manifest.frozen_wal_id >= manifest.wal_id) {
        throw std::invalid_argument("Frozen WAL must precede the active WAL");
    }
    for (const auto id : manifest.sstable_ids) {
        if (id == 0 || id >= manifest.next_file_id || id == manifest.wal_id ||
            (manifest.frozen_wal_id && id == *manifest.frozen_wal_id) || !seen.insert(id).second) {
            throw std::invalid_argument("Invalid or duplicate manifest SSTable ID");
        }
    }
}

}  // namespace

std::string EncodeManifest(const Manifest& manifest) {
    Validate(manifest);
    std::string bytes(manifest.frozen_wal_id ? "KVM2" : "KVM1");
    detail::AppendU64(bytes, manifest.wal_id);
    detail::AppendU64(bytes, manifest.next_file_id);
    detail::AppendU32(bytes, static_cast<std::uint32_t>(manifest.sstable_ids.size()));
    if (manifest.frozen_wal_id) { detail::AppendU64(bytes, *manifest.frozen_wal_id); }
    for (const auto id : manifest.sstable_ids) {
        detail::AppendU64(bytes, id);
    }
    detail::AppendU32(bytes, detail::Crc32(bytes));
    return bytes;
}

Manifest DecodeManifest(std::string_view bytes) {
    const bool frozen = bytes.substr(0, 4) == "KVM2";
    const std::size_t header_size = frozen ? 32 : 24;
    if (bytes.size() < header_size + 4 || (!frozen && bytes.substr(0, 4) != "KVM1")) {
        throw std::runtime_error("Incomplete or invalid manifest header");
    }
    const auto count = detail::ReadU32(bytes, 20);
    if (count > kMaxManifestTables || bytes.size() != header_size + 4 + 8 * static_cast<std::size_t>(count)) {
        throw std::runtime_error("Invalid manifest size or entry count");
    }
    if (detail::Crc32(bytes.substr(0, bytes.size() - 4)) != detail::ReadU32(bytes, bytes.size() - 4)) {
        throw std::runtime_error("Manifest checksum mismatch");
    }
    Manifest manifest;
    manifest.wal_id = detail::ReadU64(bytes, 4);
    manifest.next_file_id = detail::ReadU64(bytes, 12);
    if (frozen) { manifest.frozen_wal_id = detail::ReadU64(bytes, 24); }
    manifest.sstable_ids.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        manifest.sstable_ids.push_back(detail::ReadU64(bytes, header_size + 8 * i));
    }
    Validate(manifest);
    return manifest;
}

}  // namespace kvstore
