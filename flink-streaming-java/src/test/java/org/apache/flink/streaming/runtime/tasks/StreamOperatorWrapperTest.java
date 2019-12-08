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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.streaming.util.TestUtil.swapArrayElement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link StreamOperatorWrapper}.
 */
public class StreamOperatorWrapperTest extends TestLogger {

	private static SystemProcessingTimeService timerService;

	private static final int numOperators = 3;

	private List<StreamOperatorWrapper<?, ?>> operatorWrappers;

	private ConcurrentLinkedQueue<Object> output;

	private volatile StreamTask<?, ?> containingTask;

	@BeforeClass
	public static void startTimeService() {
		CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();
		timerService = new SystemProcessingTimeService(errorFuture::complete);
	}

	@AfterClass
	public static void shutdownTimeService() {
		timerService.shutdownService();
	}

	@Before
	public void setup() throws Exception {
		this.operatorWrappers = new ArrayList<>();
		this.output = new ConcurrentLinkedQueue<>();

		try (MockEnvironment env = MockEnvironment.builder().build()) {
			this.containingTask = new MockStreamTaskBuilder(env).build();

			// initialize operatorWrappers
			for (int i = 0; i < numOperators; i++) {
				MailboxExecutor mailboxExecutor = containingTask.getMailboxExecutorFactory().createExecutor(i);

				TimerMailController timerMailController = new TimerMailController(containingTask, mailboxExecutor);
				ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(
					timerService,
					timerMailController::wrapCallback);

				StreamOperatorWrapper<?, ?> operatorWrapper = new StreamOperatorWrapper<>(
					new TestOneInputStreamOperator("Operator" + i, output, processingTimeService, timerMailController),
					mailboxExecutor);
				operatorWrapper.setProcessingTimeService(processingTimeService);
				operatorWrappers.add(operatorWrapper);
			}

			for (int i = 0; i < operatorWrappers.size(); i++) {
				StreamOperatorWrapper<?, ?> operatorWrapper = operatorWrappers.get(i);
				operatorWrapper.setPrevious(i > 0 ? operatorWrappers.get(i - 1) : null);
				operatorWrapper.setNext(i < (operatorWrappers.size() - 1) ? operatorWrappers.get(i + 1) : null);
			}
		}
	}

	@After
	public void teardown() throws Exception {
		containingTask.cleanup();
	}

	@Test
	public void testCloseWithoutTransitivity() throws Exception {
		for (int i = 0; i < operatorWrappers.size(); i++) {
			output.clear();

			StreamOperatorWrapper<?, ?> operatorWrapper = operatorWrappers.get(i);
			operatorWrapper.close(containingTask, false, true);

			String prefix = "[" + "Operator" + i + "]";
			verifyOutputResult(
				new Object[]{
					prefix + ": End of input",
					prefix + ": Timer in mailbox before closing",
					prefix + ": Timer with uncertain execution order",
					prefix + ": Bye"
				},
				output.toArray()
			);

			assertTrue(getStreamOperatorFromWrapper(operatorWrapper).getTimerNotTriggered().isCancelled());
			assertThat(getStreamOperatorFromWrapper(operatorWrapper).getTimerRegisteredInClose(), is(instanceOf(NeverCompleteFuture.class)));
		}
	}

	@Test
	public void testCloseWithTransitivity() throws Exception {
		output.clear();
		operatorWrappers.get(0).close(containingTask, true);

		assertEquals((operatorWrappers.size() - 1) * 4 + 1, output.size());
		assertEquals("[Operator0]: Bye", output.poll());

		for (int i = 1; i < operatorWrappers.size(); i++) {
			Object[] result = new Object[4];
			int index = 0;
			while (index < 4) {
				result[index++] = output.poll();
			}

			String prefix = "[" + "Operator" + i + "]";
			verifyOutputResult(
				new Object[]{
					prefix + ": End of input",
					prefix + ": Timer in mailbox before closing",
					prefix + ": Timer with uncertain execution order",
					prefix + ": Bye"
				},
				result
			);
		}

		for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorWrappers) {
			assertTrue(getStreamOperatorFromWrapper(operatorWrapper).getTimerNotTriggered().isCancelled());
			assertThat(getStreamOperatorFromWrapper(operatorWrapper).getTimerRegisteredInClose(), is(instanceOf(NeverCompleteFuture.class)));
		}
	}

	@Test
	public void testClosingOperatorWithException() {
		StreamOperatorWrapper<?, ?> operatorWrapper = new StreamOperatorWrapper<>(
			new AbstractStreamOperator<Void>() {
				@Override
				public void close() throws Exception {
					throw new Exception("test exception at closing");
				}
			},
			containingTask.getMailboxExecutorFactory().createExecutor(Integer.MAX_VALUE - 1));

		try {
			operatorWrapper.close(containingTask, false);
			fail("should throw an exception");
		} catch (Throwable t) {
			Optional<Throwable> optional = ExceptionUtils.findThrowableWithMessage(t, "test exception at closing");
			assertTrue(optional.isPresent());
		}
	}

	@Test
	public void testReadIterator() {
		Iterator<StreamOperatorWrapper<?, ?>> it = new StreamOperatorWrapper.ReadIterator(operatorWrappers.get(0));
		for (int i = 0; i < operatorWrappers.size(); i++) {
			assertTrue(it.hasNext());

			StreamOperatorWrapper<?, ?> next = it.next();
			assertNotNull(next);

			TestOneInputStreamOperator operator = getStreamOperatorFromWrapper(next);
			assertEquals("Operator" + i, operator.getName());
		}
		assertFalse(it.hasNext());
	}

	@Test
	public void testReverseReadIterator() {
		Iterator<StreamOperatorWrapper<?, ?>> it = new StreamOperatorWrapper.ReverseReadIterator(operatorWrappers.get(operatorWrappers.size() - 1));
		for (int i = operatorWrappers.size() - 1; i >= 0; i--) {
			assertTrue(it.hasNext());

			StreamOperatorWrapper<?, ?> next = it.next();
			assertNotNull(next);

			TestOneInputStreamOperator operator = getStreamOperatorFromWrapper(next);
			assertEquals("Operator" + i, operator.getName());
		}
		assertFalse(it.hasNext());
	}

	private TestOneInputStreamOperator getStreamOperatorFromWrapper(StreamOperatorWrapper<?, ?> operatorWrapper) {
		return (TestOneInputStreamOperator) Objects.requireNonNull(operatorWrapper.getStreamOperator());
	}

	private static void verifyOutputResult(Object[] expected, Object[] result) {
		assertEquals(4, result.length);

		if (!result[2].toString().endsWith(": Timer with uncertain execution order")) {
			swapArrayElement(result, 2, 3);
		}
		assertArrayEquals("Output was not correct.", expected, result);
	}

	private static class TimerMailController {

		private final StreamTask<?, ?> containingTask;

		private final MailboxExecutor mailboxExecutor;

		private final ConcurrentHashMap<ProcessingTimeCallback, OneShotLatch> puttingLatches;

		private final ConcurrentHashMap<ProcessingTimeCallback, OneShotLatch> inMailboxLatches;

		TimerMailController(StreamTask<?, ?> containingTask, MailboxExecutor mailboxExecutor) {
			this.containingTask = containingTask;
			this.mailboxExecutor = mailboxExecutor;

			this.puttingLatches = new ConcurrentHashMap<>();
			this.inMailboxLatches = new ConcurrentHashMap<>();
		}

		OneShotLatch getPuttingLatch(ProcessingTimeCallback callback) {
			return puttingLatches.get(callback);
		}

		OneShotLatch getInMailboxLatch(ProcessingTimeCallback callback) {
			return inMailboxLatches.get(callback);
		}

		ProcessingTimeCallback wrapCallback(ProcessingTimeCallback callback) {
			puttingLatches.put(callback, new OneShotLatch());
			inMailboxLatches.put(callback, new OneShotLatch());

			return timestamp -> {
				puttingLatches.get(callback).trigger();
				containingTask.deferCallbackToMailbox(mailboxExecutor, callback).onProcessingTime(timestamp);
				inMailboxLatches.get(callback).trigger();
			};
		}
	}

	private static class TestOneInputStreamOperator extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String>, BoundedOneInput {

		private static final long serialVersionUID = 1L;

		private final String name;

		private final ConcurrentLinkedQueue<Object> output;

		private final ProcessingTimeService processingTimeService;

		private final TimerMailController timerMailController;

		private final ScheduledFuture<?> timerNotTriggered;

		private ScheduledFuture<?> timerRegisteredInClose;

		TestOneInputStreamOperator(
			String name,
			ConcurrentLinkedQueue<Object> output,
			ProcessingTimeService processingTimeService,
			TimerMailController timerMailController) {

			this.name = name;
			this.output = output;
			this.processingTimeService = processingTimeService;
			this.timerMailController = timerMailController;

			this.timerNotTriggered = processingTimeService.registerTimer(
				Long.MAX_VALUE, t2 -> output.add("[" + name + "]: Timer not triggered"));
		}

		public String getName() {
			return name;
		}

		ScheduledFuture<?> getTimerNotTriggered() {
			return timerNotTriggered;
		}

		ScheduledFuture<?> getTimerRegisteredInClose() {
			return timerRegisteredInClose;
		}

		@Override
		public void processElement(StreamRecord<String> element) {}

		@Override
		public void endInput() throws InterruptedException {
			output.add("[" + name + "]: End of input");

			ProcessingTimeCallback callback1 = t1 -> output.add("[" + name + "]: Timer in mailbox before closing");
			processingTimeService.registerTimer(0, callback1);

			ProcessingTimeCallback callback2 = t1 -> output.add("[" + name + "]: Timer with uncertain execution order");
			processingTimeService.registerTimer(0, callback2);

			timerMailController.getInMailboxLatch(callback1).await();
			timerMailController.getPuttingLatch(callback2).await();
		}

		@Override
		public void close() throws Exception {
			output.add("[" + name + "]: Bye");

			timerRegisteredInClose = processingTimeService.registerTimer(0, timestamp -> {});
		}
	}
}
