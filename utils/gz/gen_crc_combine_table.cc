/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

#if defined(__x86_64__) || defined(__i386__)

#include "utils/clmul.hh"
#include "barett.hh"

#include <iostream>
#include <seastar/core/print.hh>

/*
 * Returns the representation of the polynomial:
 *
 *   R_i(x) = x^(2^i*8) mod G(x)
 */
static
uint32_t get_x_pow_2pow_x_times_eight(int i) {
    uint32_t r = 0x00800000; // x^8

    while (i--) {
        //   x^(2*N)          mod G(x)
        // = (x^N)*(x^N)      mod G(x)
        // = (x^N mod G(x))^2 mod G(x)
        r = crc32_fold_barett_u64(clmul(r, r) << 1);
    }

    return r;
}

/*
 * Let t_i be the following polynomial depending on i and u:
 *
 *   t_i(x, u) = x^(u * 2^(i+3))
 *
 * where:
 *
 *   u in { 0, 1 }
 *
 * Let g_k be a multiplication modulo G(x) of t_i(x) for 8 consecutive values of i and 8 values of u (u0 ... u7):
 *
 *   g_k(x, u0, u1, ..., u7) = t_(k+0)(x, u_0) * t_(k+1)(x, u_1) * ... * t_(k+7)(x, u_7) mod G(x)
 *
 * The tables below contain representations of g_k(x) polynomials, where the bits of the index
 * correspond to coefficients of u:
 *
 * crc32_x_pow_radix_8_table_base_<k>[u] = g_k(x, (u >> 0) & 1,
 *                                                (u >> 1) & 1,
 *                                                (u >> 2) & 1,
 *                                                (u >> 3) & 1,
 *                                                (u >> 4) & 1,
 *                                                (u >> 5) & 1,
 *                                                (u >> 6) & 1,
 *                                                (u >> 7) & 1)
 */
int main() {
    const int bits = 32;
    const int radix_bits = 8;
    const uint32_t one = 0x80000000; // x^0

    std::cout << "/*\n"
                 " * Copyright (C) 2018 ScyllaDB\n"
                 " */\n"
                 "\n"
                 "/*\n"
                 " * This file is part of Scylla.\n"
                 " *\n"
                 " * Scylla is free software: you can redistribute it and/or modify\n"
                 " * it under the terms of the GNU Affero General Public License as published by\n"
                 " * the Free Software Foundation, either version 3 of the License, or\n"
                 " * (at your option) any later version.\n"
                 " *\n"
                 " * Scylla is distributed in the hope that it will be useful,\n"
                 " * but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
                 " * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
                 " * GNU General Public License for more details.\n"
                 " *\n"
                 " * You should have received a copy of the GNU General Public License\n"
                 " * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.\n"
                 " *\n"
                 " */\n"
                 "\n"
                 "#pragma once\n"
                 "\n"
                 "#include <cstdint>\n"
                 "\n"
                 "/*\n"
                 " * Generated with gen_crc_combine_table.cc\n"
                 " * DO NOT EDIT!\n"
                 " */\n"
                 "\n"
                 "/*\n"
                 " * Let t_i be the following polynomial depending on i and u:\n"
                 " *\n"
                 " *   t_i(x, u) = x^(u * 2^(i+3))\n"
                 " *\n"
                 " * where:\n"
                 " *\n"
                 " *   u in { 0, 1 }\n"
                 " *\n"
                 " * Let g_k be a product modulo G(x) of t_i(x) for 8 consecutive values of i and given 8 values of u (u0 ... u7):\n"
                 " *\n"
                 " *   g_k(x, u0, u1, ..., u7) = t_(k+0)(x, u_0) * t_(k+1)(x, u_1) * ... * t_(k+7)(x, u_7) mod G(x)\n"
                 " *\n"
                 " * The tables below contain representations of g_k(x) polynomials, where the bits of the index\n"
                 " * correspond to coefficients of u:\n"
                 " *\n"
                 " * crc32_x_pow_radix_8_table_base_<k>[u] = g_k(x, (u >> 0) & 1,\n"
                 " *                                                (u >> 1) & 1,\n"
                 " *                                                (u >> 2) & 1,\n"
                 " *                                                (u >> 3) & 1,\n"
                 " *                                                (u >> 4) & 1,\n"
                 " *                                                (u >> 5) & 1,\n"
                 " *                                                (u >> 6) & 1,\n"
                 " *                                                (u >> 7) & 1)\n"
                 " */\n";

    for (int base = 0; base < bits; base += radix_bits) {
        std::cout << "static const uint32_t crc32_x_pow_radix_8_table_base_" << base << "[] = {";

        for (int i = 0; i < (1 << radix_bits); ++i) {
            uint32_t product = one;
            for (int j = 0; j < radix_bits; ++j) {
                if (i & (1 << j)) {
                    auto r = get_x_pow_2pow_x_times_eight(base + j);
                    product = crc32_fold_barett_u64(clmul(product, r) << 1);
                }
            }
            if (i % 4 == 0) {
                std::cout << "\n    ";
            }
            std::cout << seastar::format(" 0x{:0>8x},", product);
        }

        std::cout << "\n};\n\n";
    }
}

#else

int main() {
    std::cerr << "Not implemented for this arch!\n";
    return 1;
}

#endif
