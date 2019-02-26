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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link StreamStatus} handler for the two-input stream task.
 */
public final class StreamTwoInputStreamStatusHandler implements StreamStatusHandler {

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final Input input1;
	private final Input input2;

	private final Object lock;

	/**
	 * Stream status for the two inputs. We need to keep track for determining when
	 * to forward stream status changes downstream.
	 */
	private StreamStatus streamStatus1;
	private StreamStatus streamStatus2;

	public StreamTwoInputStreamStatusHandler(
		StreamStatusMaintainer streamStatusMaintainer,
		Input input1,
		Input input2,
		Object lock) {

		this.streamStatusMaintainer = streamStatusMaintainer;
		this.input1 = input1;
		this.input2 = input2;
		this.lock = checkNotNull(lock);

	}

	@Override
	public void handleStreamStatus(Input input, StreamStatus streamStatus) {
		try {
			synchronized (lock) {
				StreamStatus anotherStreamStatus;
				if (input == input1) {
					streamStatus1 = streamStatus;
					anotherStreamStatus = streamStatus2;
				} else if (input == input2) {
					streamStatus2 = streamStatus;
					anotherStreamStatus = streamStatus1;
				} else {
					throw new RuntimeException("Unexpected input");
				}

				// check if we need to toggle the task's stream status
				if (!streamStatus.equals(streamStatusMaintainer.getStreamStatus())) {
					if (streamStatus.isActive()) {
						// we're no longer idle if at least one input has become active
						streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);
					} else if (anotherStreamStatus.isIdle()) {
						// we're idle once both inputs are idle
						streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
		}
	}
}
