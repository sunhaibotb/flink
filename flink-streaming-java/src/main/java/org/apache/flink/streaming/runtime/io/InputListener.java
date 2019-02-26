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

import org.apache.flink.annotation.Internal;

/**
 * Listener interface implemented by consumers of {@link Input} instances
 * that want to be notified when the input becomes available.
 */
@Internal
public interface InputListener {

	/**
	 * Notification callback if the input becomes available. This notification does not mean that
	 * it is guaranteed that a full stream element can be retrieved from the input.
	 *
	 * @param input Input that became available.
	 */
	void notifyInputAvailable(Input input);
}

