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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeServiceImpl.TimerScheduledFuture;
import org.apache.flink.streaming.util.TestUtil;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link ProcessingTimeServiceImpl}.
 */
public class ProcessingTimeServiceImplTest extends TestLogger {

	private static final Time testingTimeout = Time.seconds(10L);

	private SystemProcessingTimeService timerService;

	@Before
	public void setup() {
		CompletableFuture<Throwable> errorFuture = new CompletableFuture<>();

		timerService = new SystemProcessingTimeService(errorFuture::complete);
	}

	@After
	public void teardown() {
		timerService.shutdownService();
	}

	@Test
	public void testTimerRegistrationAndCancellation() throws TimeoutException, InterruptedException, ExecutionException {
		ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(timerService, v -> v);

		// test registerTimer() and cancellation
		ScheduledFuture<?> neverFiredTimer = processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {});
		assertEquals(1, processingTimeService.getNumUndoneTimers());
		assertTrue(neverFiredTimer.cancel(false));
		TestUtil.waitCondition((deadline) -> (processingTimeService.getNumUndoneTimers() == 0), testingTimeout);

		final CompletableFuture<?> firedTimerExecutionFuture = new CompletableFuture<>();
		ScheduledFuture<?> firedTimer = processingTimeService.registerTimer(0, timestamp -> firedTimerExecutionFuture.complete(null));
		firedTimer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(firedTimerExecutionFuture.isDone());
		assertFalse(firedTimer.isCancelled());
		TestUtil.waitCondition((deadline) -> (processingTimeService.getNumUndoneTimers() == 0), testingTimeout);

		final CompletableFuture<?> firingAndWaitTimerExecutionFuture = new CompletableFuture<>();
		final OneShotLatch firingAndWaitTimerLatch = new OneShotLatch();
		ScheduledFuture<?> firingAndWaitTimer = processingTimeService.registerTimer(0, timestamp -> {
			firingAndWaitTimerExecutionFuture.complete(null);
			firingAndWaitTimerLatch.await();
		});
		assertEquals(1, processingTimeService.getNumUndoneTimers());
		firingAndWaitTimerExecutionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertFalse(firingAndWaitTimer.cancel(false));
		firingAndWaitTimerLatch.trigger();
		TestUtil.waitCondition((deadline) -> (processingTimeService.getNumUndoneTimers() == 0), testingTimeout);

		// test scheduleAtFixedRate() and cancellation
		final CompletableFuture<?> periodicTimerExecutionFuture = new CompletableFuture<>();
		ScheduledFuture<?> periodicTimer = processingTimeService.scheduleAtFixedRate(
			timestamp -> periodicTimerExecutionFuture.complete(null), 0, Long.MAX_VALUE);
		assertEquals(1, processingTimeService.getNumUndoneTimers());

		periodicTimerExecutionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertTrue(periodicTimer.cancel(false));
		TestUtil.waitCondition((deadline) -> (processingTimeService.getNumUndoneTimers() == 0), testingTimeout);
	}

	@Test
	public void testQuiesceAndCancelTimersGracefully() throws Exception {
		ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(timerService, v -> v);

		final OneShotLatch firingAndWaitTimerLatch = new OneShotLatch();
		final CompletableFuture<?> firingAndWaitTimerExecutingFuture = new CompletableFuture();

		ScheduledFuture<?> neverFiredTimer = processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {});
		ScheduledFuture<?> firedTimer = processingTimeService.registerTimer(0, timestamp -> {});
		ScheduledFuture<?> firingAndWaitTimer = processingTimeService.registerTimer(0, timestamp -> {
			firingAndWaitTimerExecutingFuture.complete(null);
			firingAndWaitTimerLatch.await();
		});
		ScheduledFuture<?> periodicTimer = processingTimeService.scheduleAtFixedRate(timestamp -> {}, 0, 1);

		// wait for the "firedTimer" to be done and the "firingAndWaitTimer" to execute,
		// then quiesce the time server and cancel timers gracefully
		firedTimer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		firingAndWaitTimerExecutingFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		processingTimeService.quiesce();
		CompletableFuture<?> cancellationFuture = processingTimeService.cancelTimersGracefullyAfterQuiesce();

		// more timers are still allowed to register after the timer server is quiesced,
		// but these registered timers will never complete
		assertThat(processingTimeService.registerTimer(Long.MAX_VALUE, timestamp -> {}), is(instanceOf(NeverCompleteFuture.class)));

		// after the "firingAndWaitTimerLatch" is triggered, the cancellation future will become "done"
		assertFalse(cancellationFuture.isDone());
		firingAndWaitTimerLatch.trigger();
		cancellationFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertEquals(0, processingTimeService.getNumUndoneTimers());

		// verify the status of all timers
		assertTrue(neverFiredTimer.isCancelled());
		assertFalse(firedTimer.isCancelled());
		assertFalse(firingAndWaitTimer.isCancelled());
		assertTrue(periodicTimer.isCancelled());
	}

	@Test
	public void testQuiesceAndCancelTimersGracefullyWhenNotExistUndoneTimers() {
		ProcessingTimeServiceImpl processingTimeService = new ProcessingTimeServiceImpl(timerService, v -> v);
		processingTimeService.quiesce();
		assertTrue(processingTimeService.cancelTimersGracefullyAfterQuiesce().isDone());
	}

	/**
	 * Tests for {@link TimerScheduledFuture}.
	 */
	public static class TimerScheduledFutureTest {

		private static final Time testingTimeout = Time.seconds(10L);

		private static SystemProcessingTimeService timerService;
		private static CompletableFuture<Throwable> errorFuture;

		@BeforeClass
		public static void startTimeService() {
			errorFuture = new CompletableFuture<>();
			timerService = new SystemProcessingTimeService(errorFuture::complete);
		}

		@AfterClass
		public static void shutdownTimeService() {
			timerService.shutdownService();
		}

		@Test
		public void testOneShotTimer() throws TimeoutException, InterruptedException, ExecutionException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				false,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executionFuture = new CompletableFuture<>();
			ScheduledFuture<?> future = timerService.registerTimer(
				0,
				timer.getCallback(timestamp -> executionFuture.complete(null)));

			timer.bind(future);
			timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertTrue(future.isDone());
			assertTrue(timer.isDone());
			assertFalse(timer.isFailed());
			assertFalse(timer.isCancelled());
			assertTrue(executionFuture.isDone());
			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		@Test
		public void testOneShotTimerThatThrowsException() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				false,
				v -> onDoneActionFuture.complete(null));

			ScheduledFuture<?> future = timerService.registerTimer(
				0,
				timer.getCallback(timestamp -> {
					throw new Exception("test exception");
				}));

			timer.bind(future);
			timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertTrue(future.isDone());
			assertTrue(timer.isDone());
			assertTrue(timer.isFailed());
			assertFalse(timer.isCancelled());
			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		@Test
		public void testOneShotTimerCancelledBeforeExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				false,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executionFuture = new CompletableFuture<>();
			final OneShotLatch toFireLatch = new OneShotLatch();
			ScheduledFuture<?> future = timerService.registerTimer(
				0,
				t -> {
					toFireLatch.await();
					timer.getCallback(timestamp -> executionFuture.complete(null)).onProcessingTime(t);
				});

			timer.bind(future);
			assertTrue(timer.cancel(false));

			assertTrue(future.isCancelled());
			assertTrue(timer.isDone());
			assertTrue(timer.isCancelled());

			toFireLatch.trigger();
			try {
				timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				fail("should throw exception");
			} catch (Throwable t) {
				assertThat(t, is(instanceOf(CancellationException.class)));
			}

			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertFalse(executionFuture.isDone());
		}

		@Test
		public void testOneShotTimerCancelledOnExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				false,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executionFuture = new CompletableFuture<>();
			final OneShotLatch continuingLatch = new OneShotLatch();
			ScheduledFuture<?> future = timerService.registerTimer(
				0,
				timer.getCallback(
					timestamp -> {
						executionFuture.complete(null);
						continuingLatch.await();
					}));

			executionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			timer.bind(future);
			assertFalse(timer.cancel(false));

			continuingLatch.trigger();
			timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertTrue(timer.isDone());
			assertFalse(timer.isCancelled());
			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		@Test
		public void testOneShotTimerCancelledAfterExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				false,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executionFuture = new CompletableFuture<>();
			ScheduledFuture<?> future = timerService.registerTimer(
				0,
				timer.getCallback(timestamp -> executionFuture.complete(null)));

			timer.bind(future);
			timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertFalse(timer.cancel(false));

			assertTrue(timer.isDone());
			assertFalse(timer.isCancelled());
			assertTrue(executionFuture.isDone());
			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		@Test
		public void testPeriodicTimer() throws InterruptedException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				true,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executionFuture = new CompletableFuture<>();
			final OneShotLatch executedOnceLatch = new OneShotLatch();
			ScheduledFuture<?> future = timerService.scheduleAtFixedRate(
				t -> {
					timer.getCallback(timestamp -> executionFuture.complete(null)).onProcessingTime(t);
					executedOnceLatch.trigger();
				}, 0, Long.MAX_VALUE);

			timer.bind(future);
			executedOnceLatch.await(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertFalse(future.isDone());
			assertFalse(timer.isDone());
			assertFalse(timer.isFailed());
			assertFalse(timer.isCancelled());
			assertTrue(executionFuture.isDone());
			assertFalse(onDoneActionFuture.isDone());
		}

		@Test
		public void testPeriodicTimerThatThrowsException() throws InterruptedException, TimeoutException, ExecutionException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				true,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executedOnceFuture = new CompletableFuture<>();
			ScheduledFuture<?> future = timerService.scheduleAtFixedRate(
				t -> {
					try {
						timer.getCallback(timestamp -> {
							throw new Exception("test exception");
						}).onProcessingTime(t);
					} finally {
						executedOnceFuture.complete(null);
					}
				}, 0, Long.MAX_VALUE);

			timer.bind(future);
			executedOnceFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertFalse(future.isDone());
			assertFalse(timer.isDone());
			assertFalse(timer.isFailed());
			assertFalse(timer.isCancelled());
			assertFalse(onDoneActionFuture.isDone());
		}

		@Test
		public void testPeriodicTimerCancelledBeforeExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				true,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executionFuture = new CompletableFuture<>();
			final OneShotLatch toFireLatch = new OneShotLatch();
			ScheduledFuture<?> future = timerService.scheduleAtFixedRate(
				t -> {
					toFireLatch.await();
					timer.getCallback(timestamp -> executionFuture.complete(null)).onProcessingTime(t);
				}, 0, Long.MAX_VALUE);

			timer.bind(future);
			assertTrue(timer.cancel(false));

			assertTrue(future.isCancelled());
			assertTrue(timer.isDone());
			assertTrue(timer.isCancelled());

			toFireLatch.trigger();
			try {
				timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				fail("should throw exception");
			} catch (Throwable t) {
				assertThat(t, is(instanceOf(CancellationException.class)));
			}

			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertFalse(executionFuture.isDone());
		}

		@Test
		public void testPeriodicTimerCancelledOnExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				true,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executingFuture = new CompletableFuture<>();
			final CompletableFuture<?> executedFuture = new CompletableFuture<>();
			final OneShotLatch continuingLatch = new OneShotLatch();
			ScheduledFuture<?> future = timerService.scheduleAtFixedRate(
				timer.getCallback(
					timestamp -> {
						executingFuture.complete(null);
						continuingLatch.await();
						executedFuture.complete(null);
					}),
				0, Long.MAX_VALUE);

			executingFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			timer.bind(future);
			assertTrue(timer.cancel(false));

			assertTrue(future.isCancelled());
			assertTrue(timer.isDone());
			assertTrue(timer.isCancelled());
			assertFalse(onDoneActionFuture.isDone());

			continuingLatch.trigger();
			try {
				timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				fail("should throw exception");
			} catch (Throwable t) {
				assertThat(t, is(instanceOf(CancellationException.class)));
			}

			executedFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		@Test
		public void testPeriodicTimerCancelledAfterExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				true,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executedOnceFuture = new CompletableFuture<>();
			ScheduledFuture<?> future = timerService.scheduleAtFixedRate(
				t -> {
					timer.getCallback(timestamp -> {}).onProcessingTime(t);
					executedOnceFuture.complete(null);
				}, 0, Long.MAX_VALUE);

			timer.bind(future);
			executedOnceFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertTrue(timer.cancel(false));

			assertTrue(future.isCancelled());
			assertTrue(timer.isDone());
			assertTrue(timer.isCancelled());
			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		@Test
		public void testTimerCancelledForciblyOnExecution() throws InterruptedException, ExecutionException, TimeoutException {
			final CompletableFuture<?> onDoneActionFuture = new CompletableFuture<>();
			TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(
				false,
				v -> onDoneActionFuture.complete(null));

			final CompletableFuture<?> executingFuture = new CompletableFuture<>();
			final OneShotLatch continuingLatch = new OneShotLatch();
			ScheduledFuture<?> future = timerService.registerTimer(
				0,
				timer.getCallback(
					timestamp -> {
						executingFuture.complete(null);
						continuingLatch.await();
					}));

			executingFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			timer.bind(future);
			assertTrue(timer.cancel(true));

			assertTrue(future.isCancelled());
			assertTrue(future.isDone());
			assertTrue(timer.isDone());
			assertTrue(timer.isCancelled());

			try {
				timer.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
				fail("should throw exception");
			} catch (Throwable t) {
				assertThat(t, is(instanceOf(CancellationException.class)));
			}

			onDoneActionFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}
}
