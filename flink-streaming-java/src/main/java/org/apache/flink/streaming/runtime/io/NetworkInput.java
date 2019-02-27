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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateListener;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of taking {@link InputGate} as {@link Input}.
 */
@Internal
public class NetworkInput implements Input, InputGateListener {

	private final InputGate inputGate;

	private final DeserializationDelegate<StreamElement> deserializationDelegate;

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	/**
	 * Valves that control how watermarks and stream statuses from the input are forwarded.
	 */
	private final StatusWatermarkValve statusWatermarkValve;

	private StreamStatusHandler streamStatusHandler = null;

	/** Registered listener to forward input notifications to. */
	private volatile InputListener inputListener = null;

	/**
	 * The channel from which a buffer came, tracked so that we can appropriately map
	 * the watermarks and watermark statuses to the correct channel index of the correct valve.
	 */
	private int currentChannel = -1;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer = null;

	/** The new overall watermark aggregated on all aligned input channels. */
	private Watermark alignedWatermark = null;

	private boolean moreDeserializerDataAvailable = false;
	private volatile boolean moreInputGateDataAvailable = false;

	private final BitSet finishedChannels;

	private boolean isFinished = false;

	public NetworkInput(
		Collection<InputGate> inputGates,
		TypeSerializer<?> inputSerializer,
		IOManager ioManager) {

		this.inputGate = InputGateUtil.createInputGate(inputGates.toArray(new InputGate[inputGates.size()]));

		StreamElementSerializer<?> ser1 = new StreamElementSerializer<>(inputSerializer);
		this.deserializationDelegate = new NonReusingDeserializationDelegate<>(ser1);

		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		this.statusWatermarkValve = new StatusWatermarkValve(
			inputGate.getNumberOfInputChannels(), new ForwardingValveOutputHandler());

		this.finishedChannels = new BitSet(inputGate.getNumberOfInputChannels());

		// Register the network input as a listener for the input gate
		this.inputGate.registerListener(this);
	}

	public void setStreamStatusHandler(StreamStatusHandler streamStatusHandler) {
		if (this.streamStatusHandler == null) {
			this.streamStatusHandler = streamStatusHandler;
		} else {
			throw new IllegalStateException("Not support multiple StreamStatus handlers.");
		}
	}

	@Override
	public Optional<StreamElement> getNextElement() throws IOException, InterruptedException {
		return getNextElement(true);
	}

	@Override
	public Optional<StreamElement> pollNextElement() throws IOException, InterruptedException {
		return getNextElement(false);
	}

	private Optional<StreamElement> getNextElement(boolean blocking) throws IOException, InterruptedException {

		Optional<BufferOrEvent> next;
		while (true) {
			// get the stream element from the deserializer
			if (currentRecordDeserializer != null) {
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);
				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					moreDeserializerDataAvailable = (currentRecordDeserializer != null);

					StreamElement recordOrWatermark = deserializationDelegate.getInstance();
					if (recordOrWatermark.isWatermark()) {
						statusWatermarkValve.inputWatermark(recordOrWatermark.asWatermark(), currentChannel);
						if (alignedWatermark != null) {
							Watermark watermark = alignedWatermark;
							alignedWatermark = null;
							return Optional.of(watermark);
						}
						continue;
					} else if (recordOrWatermark.isStreamStatus()) {
						statusWatermarkValve.inputStreamStatus(recordOrWatermark.asStreamStatus(), currentChannel);
						continue;
					} else {
						return Optional.of(recordOrWatermark);
					}
				} else {
					moreDeserializerDataAvailable = false;
				}
			} else {
				moreDeserializerDataAvailable = false;
			}

			// read the buffer or event from the input gate
			moreInputGateDataAvailable = false;
			if (blocking) {
				next = inputGate.getNextBufferOrEvent();
				if (!next.isPresent()) {
					// input exhausted
					checkState(isFinished);
					return Optional.empty();
				}
			} else {
				next = inputGate.pollNextBufferOrEvent();
				if (!next.isPresent()) {
					return Optional.empty();
				}
			}
			BufferOrEvent bufferOrEvent = next.get();

			if (bufferOrEvent.moreAvailable()) {
				moreInputGateDataAvailable = true;
			}

			if (bufferOrEvent.isBuffer()) {
				currentChannel = bufferOrEvent.getChannelIndex();
				currentRecordDeserializer = recordDeserializers[currentChannel];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			} else {
				// Event received
				final AbstractEvent event = bufferOrEvent.getEvent();
				if (event.getClass() == CheckpointBarrier.class || event.getClass() == CancelCheckpointMarker.class) {
					throw new UnsupportedOperationException("Checkpoint-related events are not supported currently.");
				} else {
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}

					int channelIndex = bufferOrEvent.getChannelIndex();
					finishedChannels.set(channelIndex);
					if (finishedChannels.cardinality() == inputGate.getNumberOfInputChannels()) {
						isFinished = true;
						return Optional.empty();
					}
				}
			}
		}
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public boolean moreAvailable() {
		return !isFinished && (moreDeserializerDataAvailable || moreInputGateDataAvailable);
	}

	@Override
	public void registerListener(InputListener listener) {
		if (this.inputListener == null) {
			this.inputListener = listener;
		} else {
			throw new IllegalStateException("Not support multiple listeners.");
		}
	}

	@Override
	public void notifyInputGateNonEmpty(InputGate inputGate) {
		moreInputGateDataAvailable = true;

		if (inputListener != null) {
			inputListener.notifyInputAvailable(this);
		}
	}

	@Override
	public void close() throws Exception {
		// clear the buffers. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycleBuffer();
			}
			deserializer.clear();
		}
	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {

		private ForwardingValveOutputHandler() {

		}

		@Override
		public void handleWatermark(Watermark watermark) {
			alignedWatermark = watermark;
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			if (streamStatusHandler != null) {
				streamStatusHandler.handleStreamStatus(NetworkInput.this, streamStatus);
			}
		}
	}
}
