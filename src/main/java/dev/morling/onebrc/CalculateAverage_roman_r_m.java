/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import sun.misc.Unsafe;

import java.io.File;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class CalculateAverage_roman_r_m {

    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./src/test/resources/samples/measurements-10000-unique-keys.txt";
    private static MemorySegment ms;

    private static Unsafe UNSAFE;

    // based on http://0x80.pl/notesen/2023-03-06-swar-find-any.html
    static long hasZeroByte(long l) {
        return ((l - 0x0101010101010101L) & ~(l) & 0x8080808080808080L);
    }

    static long firstSetByteIndex(long l) {
        return ((((l - 1) & 0x101010101010101L) * 0x101010101010101L) >> 56) - 1;
    }

    static long broadcast(byte b) {
        return 0x101010101010101L * b;
    }

    static long SEMICOLON_MASK = broadcast((byte) ';');
    static long LINE_END_MASK = broadcast((byte) '\n');

    static long find(long l, long mask) {
        long xor = l ^ mask;
        long match = hasZeroByte(xor);
        return match != 0 ? firstSetByteIndex(match) : -1;
    }

    static int reverseBytes(int value) {
        return (value & 0x000000FF) << 24 | (value & 0x0000FF00) << 8 |
                (value & 0x00FF0000) >> 8 | (value & 0xFF000000) >> 24;
    }

    static long nextNewline(long from) {
        long start = from;
        long i;
        long next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        while ((i = find(next, LINE_END_MASK)) < 0) {
            start += 8;
            next = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, start);
        }
        return start + i;
    }

    public static void main(String[] args) throws Exception {
        Field f = Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        UNSAFE = (Unsafe) f.get(null);

        long fileSize = new File(FILE).length();

        var channel = FileChannel.open(Paths.get(FILE));
        ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.ofAuto());

        int numThreads = fileSize > Integer.MAX_VALUE ? Runtime.getRuntime().availableProcessors() : 1;
        long chunk = fileSize / numThreads;
        var result = IntStream.range(0, numThreads)
                .parallel()
                .mapToObj(i -> {
                    boolean lastChunk = i == numThreads - 1;
                    long chunkStart = i == 0 ? 0 : nextNewline(i * chunk) + 1;
                    long chunkEnd = lastChunk ? fileSize : nextNewline((i + 1) * chunk);

                    var resultStore = new ResultStore();
                    var station = new ByteString();

                    long offset = chunkStart;
                    while (offset < chunkEnd) {
                        long start = offset;
                        long pos = -1;

                        while (chunkEnd - offset >= 8) {
                            long next = UNSAFE.getLong(ms.address() + offset);
                            pos = find(next, SEMICOLON_MASK);
                            if (pos >= 0) {
                                offset += pos;
                                break;
                            }
                            else {
                                offset += 8;
                            }
                        }
                        if (pos < 0) {
                            while (UNSAFE.getByte(ms.address() + offset++) != ';') {
                            }
                            offset--;
                        }

                        int len = (int) (offset - start);
                        // TODO can we not copy and use a reference into the memory segment to perform table lookup?

                        station.offset = start;
                        station.len = len;
                        station.hash = 0;

                        offset++;

                        long val;
                        boolean neg = UNSAFE.getByte(ms.address() + offset) == '-';
                        offset += neg ? 1 : 0;

                        if (!lastChunk || fileSize - offset >= 8) {
                            long encodedVal = UNSAFE.getLong(ms.address() + offset);

                            // neg =
                            var lineEnd = find(encodedVal, LINE_END_MASK);
                            long mask = (1L << (8 * lineEnd)) - 1;
                            mask ^= 0xFFL << (8 * (lineEnd - 2));
                            encodedVal = Long.compress(encodedVal ^ broadcast((byte) 0x30), mask);
                            long numbers2 = reverseBytes((int) encodedVal) >> (8 * (4 - lineEnd + 1));
                            val = (numbers2 & 0xFF) + 10 * ((numbers2 >> 8) & 0xFF) + 100 * ((numbers2 >> 16) & 0xFF);
                            offset += lineEnd + 1;
                        }
                        else {
                            val = UNSAFE.getByte(ms.address() + offset++) - '0';
                            byte b;
                            while ((b = UNSAFE.getByte(ms.address() + offset++)) != '.') {
                                val = val * 10 + (b - '0');
                            }
                            b = UNSAFE.getByte(ms.address() + offset);
                            val = val * 10 + (b - '0');
                            offset += 2;
                        }

                        if (neg) {
                            val = -val;
                        }

                        var a = resultStore.get(station);
                        a.min = Math.min(a.min, val);
                        a.max = Math.max(a.max, val);
                        a.sum += val;
                        a.count++;
                    }

                    // System.out.println(STR."Thread \{i}, misses=\{resultStore.misses}/\{resultStore.calls} \{((double) resultStore.misses) / resultStore.calls}");
                    return resultStore.toMap();
                }).reduce((m1, m2) -> {
                    m2.forEach((k, v) -> m1.merge(k, v, ResultRow::merge));
                    return m1;
                });

        System.out.println(result.get());
    }

    static final class ByteString {

        private long offset;
        private int len = 0;
        private int hash = 0;

        @Override
        public String toString() {
            var bytes = new byte[len];
            UNSAFE.copyMemory(null, ms.address() + offset, bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
            return new String(bytes, 0, len);
        }

        public ByteString copy() {
            var copy = new ByteString();
            copy.offset = this.offset;
            copy.len = this.len;
            copy.hash = this.hash;
            return copy;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            ByteString that = (ByteString) o;

            if (len != that.len)
                return false;

            int i = 0;

            long base1 = ms.address() + offset;
            long base2 = ms.address() + that.offset;
            for (; i + 3 < len; i += 4) {
                int i1 = UNSAFE.getInt(base1 + i);
                int i2 = UNSAFE.getInt(base2 + i);
                if (i1 != i2) {
                    return false;
                }
            }
            for (; i < len; i++) {
                byte i1 = UNSAFE.getByte(base1 + i);
                byte i2 = UNSAFE.getByte(base2 + i);
                if (i1 != i2) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            if (hash == 0) {
                long h = UNSAFE.getLong(ms.address() + offset);
                h = Long.reverseBytes(h) >>> (8 * Math.max(0, 8 - len));
                hash = (int) (h ^ (h >>> 32));
            }
            return hash;
        }
    }

    private static final class ResultRow {
        long min = 1000;
        long sum = 0;
        long max = -1000;
        int count = 0;

        public String toString() {
            return round(min / 10.0) + "/" + round(sum / 10.0 / count) + "/" + round(max / 10.0);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public ResultRow merge(ResultRow other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
            return this;
        }
    }

    static class ResultStore {
        private static final int SIZE = 16384;
        private final ByteString[] keys = new ByteString[SIZE];
        private final ResultRow[] values = new ResultRow[SIZE];

        // private long calls = 0;
        // private long misses = 0;

        ResultRow get(ByteString s) {
            int h = s.hashCode();
            int idx = (SIZE - 1) & h;

            int i = 0;
            // int miss = 0;
            while (keys[idx] != null && !keys[idx].equals(s)) {
                // miss = 1;
                i++;
                idx = (idx + i * i) % SIZE;
            }
            ResultRow result;
            if (keys[idx] == null) {
                keys[idx] = s.copy();
                result = new ResultRow();
                values[idx] = result;
            }
            else {
                result = values[idx];
                // TODO see it it makes any difference
                // keys[idx].offset = s.offset;
            }
            // calls++;
            // misses += miss;
            return result;
        }

        TreeMap<String, ResultRow> toMap() {
            var result = new TreeMap<String, ResultRow>();
            for (int i = 0; i < SIZE; i++) {
                if (keys[i] != null) {
                    result.put(keys[i].toString(), values[i]);
                }
            }
            return result;
        }
    }
}
