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

package org.apache.flink.streaming.util;

import org.apache.flink.api.common.time.Time;

import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

/**
 * Test utilities.
 */
public class TestUtil {

	public static void waitCondition(Predicate<Long> condition, Time timeout) throws InterruptedException, TimeoutException {
		waitCondition(condition, timeout, 2);
	}

	public static void waitCondition(Predicate<Long> condition, Time timeout, long retryDelayInMs)
		throws InterruptedException, TimeoutException {

		final long deadline = System.currentTimeMillis() + timeout.toMilliseconds();

		boolean result = false;
		while ((deadline - System.currentTimeMillis()) > 0) {
			result = condition.test(deadline);
			if (result) {
				break;
			}
			Thread.sleep(retryDelayInMs);
		}

		if (!result) {
			throw new TimeoutException();
		}
	}

	public static <T> void swapArrayElement(T[] array, int i, int j) {
		T temp = array[i];
		array[i] = array[j];
		array[j] = temp;
	}
}
