/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.NeverCompleteFuture;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@Internal
class ProcessingTimeServiceImpl implements ProcessingTimeService {

	private static final int STATUS_ALIVE = 0;
	private static final int STATUS_QUIESCED = 1;

	// ------------------------------------------------------------------------

	private final TimerService timerService;

	private final Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper;

	private final ConcurrentHashMap<TimerScheduledFuture<?>, Object> undoneTimers;

	private final CompletableFuture<Void> timersDoneFutureAfterQuiescing;

	private final AtomicInteger status;

	ProcessingTimeServiceImpl(
			TimerService timerService,
			Function<ProcessingTimeCallback, ProcessingTimeCallback> processingTimeCallbackWrapper) {
		this.timerService = timerService;
		this.processingTimeCallbackWrapper = processingTimeCallbackWrapper;

		this.undoneTimers = new ConcurrentHashMap<>();
		this.timersDoneFutureAfterQuiescing = new CompletableFuture<>();
		this.status = new AtomicInteger(STATUS_ALIVE);
	}

	@Override
	public long getCurrentProcessingTime() {
		return timerService.getCurrentProcessingTime();
	}

	@Override
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		if (isQuiesced()) {
			return new NeverCompleteFuture(
				ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime()));
		}

		final TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(false, this::removeUndoneTimer);
		undoneTimers.put(timer, Boolean.TRUE);

		// double check to deal with the following race conditions:
		// 1. canceling timers from the undone table occurs before putting this timer into the undone table
		//    (see #cancelTimersNotInExecuting())
		// 2. using the size of the undone table to determine if all timers have done occurs before putting
		//    this timer into the undone table (see #tryCompleteTimersDoneFutureIfQuiesced())
		if (isQuiesced()) {
			removeUndoneTimer(timer);
			return new NeverCompleteFuture(
				ProcessingTimeServiceUtil.getProcessingTimeDelay(timestamp, getCurrentProcessingTime()));
		}

		timer.bind(timerService.registerTimer(timestamp, timer.getCallback(processingTimeCallbackWrapper.apply(target))));

		return timer;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		if (isQuiesced()) {
			return new NeverCompleteFuture(initialDelay);
		}

		final TimerScheduledFuture<?> timer = new TimerScheduledFuture<Void>(true, this::removeUndoneTimer);
		undoneTimers.put(timer, Boolean.TRUE);

		// double check to deal with the following race conditions:
		// 1. canceling timers from the undone table occurs before putting this timer into the undone table
		//    (see #cancelTimersNotInExecuting())
		// 2. using the size of the undone table to determine if all timers have done occurs before putting
		//    this timer into the undone table (see #tryCompleteTimersDoneFutureIfQuiesced())
		if (isQuiesced()) {
			removeUndoneTimer(timer);
			return new NeverCompleteFuture(initialDelay);
		}

		timer.bind(
			timerService.scheduleAtFixedRate(
				timer.getCallback(processingTimeCallbackWrapper.apply(callback)), initialDelay, period));

		return timer;
	}

	void quiesce() {
		status.compareAndSet(STATUS_ALIVE, STATUS_QUIESCED);
	}

	/**
	 * This is an idempotent method to allow to repeatedly call.
	 */
	CompletableFuture<Void> cancelTimersGracefullyAfterQuiesce() {
		checkState(status.get() == STATUS_QUIESCED);

		if (!timersDoneFutureAfterQuiescing.isDone()) {
			if (!cancelTimersNotInExecuting()) {
				return FutureUtils.completedExceptionally(new CancellationException("Cancel timers failed"));
			}
			tryCompleteTimersDoneFutureIfQuiesced();
		}

		return timersDoneFutureAfterQuiescing;
	}

	@VisibleForTesting
	int getNumUndoneTimers() {
		return undoneTimers.size();
	}

	private boolean isQuiesced() {
		return status.get() == STATUS_QUIESCED;
	}

	private void removeUndoneTimer(TimerScheduledFuture<?> timer) {
		undoneTimers.remove(timer);
		tryCompleteTimersDoneFutureIfQuiesced();
	}

	private void tryCompleteTimersDoneFutureIfQuiesced() {
		if (isQuiesced() && getNumUndoneTimers() == 0) {
			timersDoneFutureAfterQuiescing.complete(null);
		}
	}

	private boolean cancelTimersNotInExecuting() {
		// we should cancel the timers in descending timestamp order
		TimerScheduledFuture<?>[] timers = undoneTimers.keySet().toArray(new TimerScheduledFuture<?>[0]);
		Arrays.sort(timers, Comparator.reverseOrder());
		for (TimerScheduledFuture<?> timer : timers) {
			if (!timer.cancel(false) && !timer.canDoneIfCancellationFailure()) {
				return false;
			}
		}
		return true;
	}

	static final class TimerScheduledFuture<V> implements ScheduledFuture<V> {

		private static final int STATUS_PENDING = 0;
		private static final int STATUS_EXECUTING = 1;
		private static final int STATUS_CANCELLED = 2;
		private static final int STATUS_FAILED = 3;
		private static final int STATUS_SUCCEEDED = 4;

		// ------------------------------------------------------------------------

		private final CompletableFuture<ScheduledFuture<?>> wrappedFuture;

		private final boolean isPeriodic;

		private final CompletableFuture<TimerScheduledFuture<?>> onDoneActionFuture;

		/**
		 * This is used to control execution behavior to ensure that the on-done action is executed
		 * after {@link ProcessingTimeCallback#onProcessingTime(long)}.
		 */
		private final AtomicInteger status;

		TimerScheduledFuture(boolean isPeriodic, Consumer<TimerScheduledFuture<?>> onDoneAction) {
			this.isPeriodic = isPeriodic;

			this.wrappedFuture = new CompletableFuture<>();
			this.onDoneActionFuture = new CompletableFuture<>();
			this.status = new AtomicInteger(STATUS_PENDING);

			// if "onDoneAction" is executed in the timer thread, the execution of "onDoneAction" may
			// be interrupted when canceling the timer, which will cause some state uncertainty, so we
			// execute it asynchronously
			onDoneActionFuture.thenAcceptAsync(checkNotNull(onDoneAction));
		}

		void bind(ScheduledFuture<?> future) {
			checkState(!this.wrappedFuture.isDone());
			this.wrappedFuture.complete(future);
		}

		ProcessingTimeCallback getCallback(ProcessingTimeCallback callback) {
			return isPeriodic ? wrapPeriodicCallback(callback) : wrapOneShotCallback(callback);
		}

		private ProcessingTimeCallback wrapOneShotCallback(ProcessingTimeCallback callback) {

			return timestamp -> {
				if (status.compareAndSet(STATUS_PENDING, STATUS_EXECUTING)) {

					try {
						callback.onProcessingTime(timestamp);

						status.compareAndSet(STATUS_EXECUTING, STATUS_SUCCEEDED);
					} catch (Throwable throwable) {
						// this may be a true execution failure or a cancellation
						// that can be interrupted to running

						status.compareAndSet(STATUS_EXECUTING, STATUS_FAILED);
						throw throwable;
					} finally {
						onDoneActionFuture.complete(this);
					}
				}
			};
		}

		private ProcessingTimeCallback wrapPeriodicCallback(ProcessingTimeCallback callback) {

			return timestamp -> {
				if (status.compareAndSet(STATUS_PENDING, STATUS_EXECUTING)) {

					callback.onProcessingTime(timestamp);
					if (!status.compareAndSet(STATUS_EXECUTING, STATUS_PENDING)) {
						checkState(status.get() == STATUS_CANCELLED);
						onDoneActionFuture.complete(this);
					}
				} else if (status.get() == STATUS_CANCELLED) {
					// when the underlying future is not successfully cancelled, this ensures that it
					// will not run at idle all the time
					// (see the logic of canceling from the "STATUS_PENDING" state in #cancel())
					checkState(wrappedFuture.isDone());
					wrappedFuture.get().cancel(false);
				}
			};
		}

		private ScheduledFuture<?> getUnderlyingFuture() {
			try {
				return wrappedFuture.get();
			} catch (Throwable t) {
				throw new RuntimeException(t);
			}
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			ScheduledFuture<?> future = getUnderlyingFuture();

			if (status.compareAndSet(STATUS_PENDING, STATUS_CANCELLED)) {
				// we don't need to worry about whether the cancellation is successful, even if it is
				// not, the callback that is wrapped will not be executed
				future.cancel(mayInterruptIfRunning);

				onDoneActionFuture.complete(this);
				return true;
			}

			if (mayInterruptIfRunning) {
				if (future.cancel(true)) {
					status.set(STATUS_CANCELLED);
					onDoneActionFuture.complete(this);

					return true;
				}
			} else if (isPeriodic) {
				if (future.cancel(false)) {
					// if the timer is still executing, "onDoneActionFuture" must be completed by the
					// timer thread to ensure that "onDoneAction" runs at the right time
					if (!status.compareAndSet(STATUS_EXECUTING, STATUS_CANCELLED)) {
						checkState(status.compareAndSet(STATUS_PENDING, STATUS_CANCELLED));
						onDoneActionFuture.complete(this);
					}

					return true;
				}
			}

			return false;
		}

		@VisibleForTesting
		boolean isFailed() {
			return status.get() == STATUS_FAILED;
		}

		@VisibleForTesting
		boolean isPending() {
			return status.get() == STATUS_PENDING;
		}

		boolean canDoneIfCancellationFailure() {
			return !isPeriodic && !isPending();
		}

		@Override
		public boolean isCancelled() {
			return status.get() == STATUS_CANCELLED;
		}

		@Override
		public boolean isDone() {
			return isCancelled() || getUnderlyingFuture().isDone();
		}

		@Override
		public long getDelay(@Nonnull TimeUnit unit) {
			return getUnderlyingFuture().getDelay(unit);
		}

		@Override
		public int compareTo(@Nonnull Delayed o) {
			return getUnderlyingFuture().compareTo(o);
		}

		@SuppressWarnings("unchecked")
		@Override
		public V get() throws InterruptedException, ExecutionException {
			return (V) getUnderlyingFuture().get();
		}

		@SuppressWarnings("unchecked")
		@Override
		public V get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			return (V) getUnderlyingFuture().get(timeout, unit);
		}
	}
}
