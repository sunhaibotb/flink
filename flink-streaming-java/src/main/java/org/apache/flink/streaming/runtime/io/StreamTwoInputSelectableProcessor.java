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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.BoundedTwoInput;
import org.apache.flink.streaming.api.operators.InputIdentifier;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.operators.TwoInputSelectable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask}
 * in the case that the operator is TwoInputSelectable.
 *
 * @param <IN1> The type of the records that arrive on the first input
 * @param <IN2> The type of the records that arrive on the second input
 */
@Internal
public class StreamTwoInputSelectableProcessor<IN1, IN2> implements InputListener {

	private static final Logger LOG = LoggerFactory.getLogger(StreamTwoInputSelectableProcessor.class);

	private final Input input1;
	private final Input input2;

	private final Object lock;

	private InputSelection inputSelection;

	private final TwoInputStreamOperator<IN1, IN2, ?> streamOperator;

	/** Inputs, which notified this input about available data. */
	private final ArrayDeque<Input> inputsWithData = new ArrayDeque<>();

	/**
	 * Guardian against enqueuing an {@link Input} multiple times on {@code inputsWithData}.
	 */
	private final Set<Input> enqueuedInputsWithData = new HashSet<>();

	private boolean isFinished1 = false;
	private boolean isFinished2 = false;

	// ---------------- Metrics ------------------

	private final WatermarkGauge input1WatermarkGauge;
	private final WatermarkGauge input2WatermarkGauge;

	private Counter numRecordsIn;

	public StreamTwoInputSelectableProcessor(
		Collection<InputGate> inputGates1,
		Collection<InputGate> inputGates2,
		TypeSerializer<IN1> inputSerializer1,
		TypeSerializer<IN2> inputSerializer2,
		Object lock,
		IOManager ioManager,
		StreamStatusMaintainer streamStatusMaintainer,
		TwoInputStreamOperator<IN1, IN2, ?> streamOperator,
		WatermarkGauge input1WatermarkGauge,
		WatermarkGauge input2WatermarkGauge) {

		checkState(streamOperator instanceof TwoInputSelectable);

		this.input1 = new NetworkInput(inputGates1, inputSerializer1, ioManager);
		this.input2 = new NetworkInput(inputGates2, inputSerializer2, ioManager);

		StreamTwoInputStreamStatusHandler streamStatusHandler = new StreamTwoInputStreamStatusHandler(
			streamStatusMaintainer, input1, input2, lock);
		((NetworkInput) input1).setStreamStatusHandler(streamStatusHandler);
		((NetworkInput) input2).setStreamStatusHandler(streamStatusHandler);

		this.lock = checkNotNull(lock);

		this.streamOperator = streamOperator;

		this.input1WatermarkGauge = input1WatermarkGauge;
		this.input2WatermarkGauge = input2WatermarkGauge;
	}

	/**
	 * Notes that it must be called after calling all operator's open(). This ensures that
	 * the first input selection determined by the operator at open and before is valid.
	 */
	public void init() {
		inputSelection = ((TwoInputSelectable) streamOperator).nextSelection();

		if (numRecordsIn == null) {
			try {
				numRecordsIn = ((OperatorMetricGroup) streamOperator
					.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				numRecordsIn = new SimpleCounter();
			}
		}

		input1.registerListener(this);
		input2.registerListener(this);
	}

	public boolean processInput() throws Exception {
		if (isFinished1 && isFinished2) {
			return false;
		}

		Input currentInput;
		Optional<StreamElement> next;
		while (true) {
			// read the data according to inputSelection
			if (InputSelection.FIRST.equals(inputSelection)) {
				checkState(!isFinished1, "Could not read a finished input: input1");

				currentInput = dequeueInput(input1);
				next = currentInput.getNextElement();
			} else if (InputSelection.SECOND.equals(inputSelection)) {
				checkState(!isFinished2, "Could not read a finished input: input2");

				currentInput = dequeueInput(input2);
				next = currentInput.getNextElement();
			} else {
				// it's ANY
				synchronized (inputsWithData) {
					while (inputsWithData.size() == 0) {
						inputsWithData.wait();
					}
					currentInput = inputsWithData.remove();
					enqueuedInputsWithData.remove(currentInput);
				}
				next = currentInput.pollNextElement();
			}

			// we re-add the current input in case it has more data, because in that case no "available"
			// notification will come for that input
			if (currentInput.moreAvailable()) {
				queueInput(currentInput);
			}

			// the current input may be finished
			if (!next.isPresent()) {
				if (currentInput.isFinished()) {
					if (currentInput == input1) {
						if (streamOperator instanceof BoundedTwoInput) {
							((BoundedTwoInput) streamOperator).endInput(InputIdentifier.FIRST);
						}

						inputSelection = InputSelection.SECOND;
						isFinished1 = true;
					} else {
						if (streamOperator instanceof BoundedTwoInput) {
							((BoundedTwoInput) streamOperator).endInput(InputIdentifier.SECOND);
						}

						inputSelection = InputSelection.FIRST;
						isFinished2 = true;
					}

					if (isFinished1 && isFinished2) {
						return false;
					}
				}

				continue;
			}

			// process the stream element
			StreamElement recordOrWatermark = next.get();
			if (currentInput == input1) {
				if (recordOrWatermark.isRecord()) {
					StreamRecord<IN1> record = recordOrWatermark.asRecord();
					synchronized (lock) {
						numRecordsIn.inc();
						streamOperator.setKeyContextElement1(record);
						streamOperator.processElement1(record);
						inputSelection = ((TwoInputSelectable) streamOperator).nextSelection();
					}
					return true;
				} else if (recordOrWatermark.isWatermark()) {
					Watermark watermark = recordOrWatermark.asWatermark();
					synchronized (lock) {
						input1WatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
						streamOperator.processWatermark1(watermark);
					}
					continue;
				} else if (recordOrWatermark.isLatencyMarker()) {
					synchronized (lock) {
						streamOperator.processLatencyMarker1(recordOrWatermark.asLatencyMarker());
					}
					continue;
				} else {
					throw new IOException("Unexpected element type on input1: "
						+ recordOrWatermark.getClass().getName());
				}
			} else {
				if (recordOrWatermark.isRecord()) {
					StreamRecord<IN2> record = recordOrWatermark.asRecord();
					synchronized (lock) {
						numRecordsIn.inc();
						streamOperator.setKeyContextElement2(record);
						streamOperator.processElement2(record);
						inputSelection = ((TwoInputSelectable) streamOperator).nextSelection();
					}
					return true;
				} else if (recordOrWatermark.isWatermark()) {
					Watermark watermark = recordOrWatermark.asWatermark();
					synchronized (lock) {
						input2WatermarkGauge.setCurrentWatermark(watermark.getTimestamp());
						streamOperator.processWatermark2(watermark);
					}
					continue;
				} else if (recordOrWatermark.isLatencyMarker()) {
					synchronized (lock) {
						streamOperator.processLatencyMarker2(recordOrWatermark.asLatencyMarker());
					}
					continue;
				} else {
					throw new IOException("Unexpected element type on input2: "
						+ recordOrWatermark.getClass().getName());
				}
			}
		}
	}

	@Override
	public void notifyInputAvailable(Input input) {
		queueInput(checkNotNull(input));
	}

	private void queueInput(Input input) {
		int availableInputs;

		synchronized (inputsWithData) {
			if (enqueuedInputsWithData.contains(input)) {
				return;
			}

			availableInputs = inputsWithData.size();

			inputsWithData.add(input);
			enqueuedInputsWithData.add(input);

			if (availableInputs == 0) {
				inputsWithData.notifyAll();
			}
		}
	}

	private Input dequeueInput(Input input) {
		synchronized (inputsWithData) {
			if (enqueuedInputsWithData.contains(input)) {
				inputsWithData.remove(input);
				enqueuedInputsWithData.remove(input);
			}
		}

		return input;
	}

	public void cleanup() {
		try {
			if (input1 != null) {
				input1.close();
			}
		} catch (Throwable t) {
			LOG.warn("An exception occurred when closing input1.", t);
		}

		try {
			if (input2 != null) {
				input2.close();
			}
		} catch (Throwable t) {
			LOG.warn("An exception occurred when closing input2.", t);
		}
	}
}
