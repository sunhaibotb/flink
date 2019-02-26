/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Describe the selection of operator's input to read.
 */
@PublicEvolving
public final class InputSelection {
	/**
	 * The {@code InputSelection} object corresponding to the input
	 * number {@code -1}.
	 */
	public static final InputSelection ANY = new InputSelection(-1);

	/**
	 * The {@code InputSelection} object corresponding to the input
	 * number {@code 1}.
	 */
	public static final InputSelection FIRST = new InputSelection(1);

	/**
	 * The {@code InputSelection} object corresponding to the input
	 * number {@code 2}.
	 */
	public static final InputSelection SECOND = new InputSelection(2);

	/**
	 * The number of the selected input. It's numbered from {@code 1},
	 * and the value {@code 1} indicates the first input. The special
	 * value {@code -1} means that do not take active selection, so it
	 * is any.
	 */
	private final int inputNumber;

	public InputSelection(int inputNumber) {
		this.inputNumber = inputNumber;
	}

	public int getInputNumber() {
		return inputNumber;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		InputSelection that = (InputSelection) o;
		return inputNumber == that.inputNumber;
	}

	@Override
	public String toString() {
		return String.valueOf(inputNumber);
	}
}
