/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.util;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;

/**
 * Util for binary row. Many of the methods in this class are used in code generation.
 * So ignore IDE warnings.
 */
public class BinaryRowUtil {

	public static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	public static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	public static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);

	public static final BinaryRow EMPTY_ROW = new BinaryRow(0);

	static {
		int size = EMPTY_ROW.getFixedLengthPartSize();
		byte[] bytes = new byte[size];
		EMPTY_ROW.pointTo(MemorySegmentFactory.wrap(bytes), 0, size);
	}

	public static boolean byteArrayEquals(byte[] left, byte[] right, int length) {
		return byteArrayEquals(
			left, BYTE_ARRAY_BASE_OFFSET, right, BYTE_ARRAY_BASE_OFFSET, length);
	}

	public static boolean byteArrayEquals(
		Object left, long leftOffset, Object right, long rightOffset, int length) {
		int i = 0;

		while (i <= length - 8) {
			if (UNSAFE.getLong(left, leftOffset + i) !=
				UNSAFE.getLong(right, rightOffset + i)) {
				return false;
			}
			i += 8;
		}

		while (i < length) {
			if (UNSAFE.getByte(left, leftOffset + i) !=
				UNSAFE.getByte(right, rightOffset + i)) {
				return false;
			}
			i += 1;
		}
		return true;
	}

	public static int hashInt(int value) {
		return Integer.hashCode(value);
	}

	public static int hashLong(long value) {
		return Long.hashCode(value);
	}

	public static int hashShort(short value) {
		return Short.hashCode(value);
	}

	public static int hashByte(byte value) {
		return Byte.hashCode(value);
	}

	public static int hashFloat(float value) {
		return Float.hashCode(value);
	}

	public static int hashDouble(double value) {
		return Double.hashCode(value);
	}

	public static int hashBoolean(boolean value) {
		return Boolean.hashCode(value);
	}

	public static int hashChar(char value) {
		return Character.hashCode(value);
	}

	public static int hashObject(Object value) {
		return value.hashCode();
	}

	public static int hashString(BinaryString value) {
		return value.hashCode();
	}

	public static int hashDecimal(Decimal value) {
		return value.hashCode();
	}

}
