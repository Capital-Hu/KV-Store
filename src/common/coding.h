#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace kvstore::detail {

inline void AppendU32(std::string& bytes, std::uint32_t value) {
    for (int shift = 0; shift < 32; shift += 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xffU));
    }
}

inline void AppendU64(std::string& bytes, std::uint64_t value) {
    for (int shift = 0; shift < 64; shift += 8) {
        bytes.push_back(static_cast<char>((value >> shift) & 0xffU));
    }
}

// Callers must check bounds before reading an integer.
inline std::uint32_t ReadU32(std::string_view bytes, std::size_t offset) {
    std::uint32_t value = 0;
    for (int shift = 0; shift < 32; shift += 8) {
        value |= static_cast<std::uint32_t>(static_cast<unsigned char>(bytes[offset++])) << shift;
    }
    return value;
}

inline std::uint64_t ReadU64(std::string_view bytes, std::size_t offset) {
    std::uint64_t value = 0;
    for (int shift = 0; shift < 64; shift += 8) {
        value |= static_cast<std::uint64_t>(static_cast<unsigned char>(bytes[offset++])) << shift;
    }
    return value;
}

// CRC-32 (IEEE), reflected polynomial; see RFC 1952, section 8.
inline std::uint32_t Crc32(std::string_view bytes) {
    std::uint32_t crc = 0xffffffffU;
    for (unsigned char byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) {
            crc = (crc & 1U) != 0 ? (crc >> 1) ^ 0xedb88320U : crc >> 1;
        }
    }
    return crc ^ 0xffffffffU;
}

}  // namespace kvstore::detail
