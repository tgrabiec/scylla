/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "murmur_hash.hh"

namespace utils {

namespace murmur_hash {

uint32_t hash32(bytes_view data, uint32_t seed)
{
    uint32_t length = data.size();
    uint32_t m = 0x5bd1e995;
    uint32_t r = 24;

    uint32_t h = seed ^ length;

    uint32_t len_4 = length >> 2;

    for (uint32_t i = 0; i < len_4; i++)
    {
        uint32_t i_4 = i << 2;
        uint32_t k = data[i_4 + 3];
        k = k << 8;
        k = k | (data[i_4 + 2] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 1] & 0xff);
        k = k << 8;
        k = k | (data[i_4 + 0] & 0xff);
        k *= m;
        k ^= (uint32_t)k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    // avoid calculating modulo
    uint32_t len_m = len_4 << 2;
    uint32_t left = length - len_m;

    if (left != 0)
    {
        if (left >= 3)
        {
            h ^= (uint32_t) data[length - 3] << 16;
        }
        if (left >= 2)
        {
            h ^= (uint32_t) data[length - 2] << 8;
        }
        if (left >= 1)
        {
            h ^= (uint32_t) data[length - 1];
        }

        h *= m;
    }

    h ^= (uint32_t)h >> 13;
    h *= m;
    h ^= (uint32_t)h >> 15;

    return h;
}

uint64_t hash2_64(bytes_view key, uint64_t seed)
{
    uint32_t length = key.size();
    uint64_t m64 = 0xc6a4a7935bd1e995L;
    uint32_t r64 = 47;

    uint64_t h64 = (seed & 0xffffffffL) ^ (m64 * length);

    uint32_t lenLongs = length >> 3;

    for (uint32_t i = 0; i < lenLongs; ++i)
    {
        uint32_t i_8 = i << 3;

        uint64_t k64 =  ((uint64_t)  key[i_8+0] & 0xff)      + (((uint64_t) key[i_8+1] & 0xff)<<8)  +
                (((uint64_t) key[i_8+2] & 0xff)<<16) + (((uint64_t) key[i_8+3] & 0xff)<<24) +
                (((uint64_t) key[i_8+4] & 0xff)<<32) + (((uint64_t) key[i_8+5] & 0xff)<<40) +
                (((uint64_t) key[i_8+6] & 0xff)<<48) + (((uint64_t) key[i_8+7] & 0xff)<<56);

        k64 *= m64;
        k64 ^= k64 >> r64;
        k64 *= m64;

        h64 ^= k64;
        h64 *= m64;
    }

    uint32_t rem = length & 0x7;

    switch (rem)
    {
    case 0:
        break;
    case 7:
        h64 ^= (uint64_t) key[length - rem + 6] << 48;
        [[fallthrough]];
    case 6:
        h64 ^= (uint64_t) key[length - rem + 5] << 40;
        [[fallthrough]];
    case 5:
        h64 ^= (uint64_t) key[length - rem + 4] << 32;
        [[fallthrough]];
    case 4:
        h64 ^= (uint64_t) key[length - rem + 3] << 24;
        [[fallthrough]];
    case 3:
        h64 ^= (uint64_t) key[length - rem + 2] << 16;
        [[fallthrough]];
    case 2:
        h64 ^= (uint64_t) key[length - rem + 1] << 8;
        [[fallthrough]];
    case 1:
        h64 ^= (uint64_t) key[length - rem];
        h64 *= m64;
    }

    h64 ^= (uint64_t)h64 >> r64;
    h64 *= m64;
    h64 ^= (uint64_t)h64 >> r64;

    return h64;
}

void hash3_x64_128(bytes_view key, uint64_t seed, std::array<uint64_t,2> &result)
{
    uint32_t length = key.size();
    const uint32_t nblocks = length >> 4; // Process as 128-bit blocks.

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    //----------
    // body

    for(uint32_t i = 0; i < nblocks; i++)
    {
        hash3_x64_128_block(h1, h2, key.data() + i * 16);
    }

    //----------
    // tail and finalization

    hash3_x64_128_finish(h1, h2, key.data() + nblocks * 16, length, result);
}

} // namespace murmur_hash
} // namespace utils
