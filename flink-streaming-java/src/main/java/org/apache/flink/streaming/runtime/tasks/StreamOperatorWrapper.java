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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;

import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class wraps a {@link StreamOperator} and handles its close, endInput and other related logic.
 * It also supports automatically transmitting the close operation to all operators on the operator chain
 * behind it in the topological order.
 *
 * <p>Before closing the wrapped operator, it quiesces its processing time service, cancels the timers
 * which registered but not yet scheduled for execution, and waits for all pending (executing) timers to
 * finish.
 */
@Internal
public class StreamOperatorWrapper<OUT, OP extends StreamOperator<OUT>> {

	@Nullable
	private final OP wrapped;

	private final MailboxExecutor mailboxExecutor;

	private StreamOperatorWrapper<?, ?> previous;

	private StreamOperatorWrapper<?, ?> next;

	private ProcessingTimeServiceImpl processingTimeService;

	StreamOperatorWrapper(OP wrapped, MailboxExecutor mailboxExecutor) {
		this.wrapped = wrapped;
		this.mailboxExecutor = checkNotNull(mailboxExecutor);
	}

	public void close(StreamTask<?, ?> containingTask, boolean transitive) throws Exception {
		close(containingTask, transitive, false);
	}

	/**
	 * Ends an input of the operator contained by this wrapper.
	 *
	 * @param inputId the input ID starts from 1 which indicates the first input.
	 */
	public void endOperatorInput(int inputId) throws Exception {
		if (wrapped instanceof BoundedOneInput) {
			((BoundedOneInput) wrapped).endInput();
		} else if (wrapped instanceof BoundedMultiInput) {
			((BoundedMultiInput) wrapped).endInput(inputId);
		}
	}

	public OP getStreamOperator() {
		return wrapped;
	}

	public MailboxExecutor getMailboxExecutor() {
		return mailboxExecutor;
	}

	public ProcessingTimeService getProcessingTimeService() {
		return processingTimeService;
	}

	void setProcessingTimeService(ProcessingTimeServiceImpl processingTimeService) {
		this.processingTimeService = processingTimeService;
	}

	void setPrevious(StreamOperatorWrapper previous) {
		this.previous = previous;
	}

	void setNext(StreamOperatorWrapper next) {
		this.next = next;
	}

	@VisibleForTesting
	void close(StreamTask<?, ?> containingTask, boolean transitive, boolean invokingEndInput) throws Exception {
		if (invokingEndInput) {
			// NOTE: This only do for the case where the operator is one-input operator. At present,
			// any non-head operator on the operator chain is one-input operator.
			synchronized (containingTask.getCheckpointLock()) {
				endOperatorInput(1);
			}
		}

		closeGracefully(containingTask);

		// transmit the close operation to the next operator on the operator chain in the topological order
		if (transitive && next != null) {
			next.close(containingTask, true, true);
		}
	}

	private void closeGracefully(StreamTask<?, ?> containingTask) throws InterruptedException, ExecutionException {
		CompletableFuture<Void> closedFuture = new CompletableFuture<>();

		// make sure no new processing-time timers come in
		quiesceProcessingTimeService();

		// 1. executing the close operation must be deferred to the mailbox to ensure that mails already
		//    in the mailbox are finished before closing the operator
		// 2. to ensure that there is no longer output triggered by the processing-time timers before invoke
		//    the "endInput" methods of downstream operators in the operator chain, we must cancel timers not
		//    in executing after closing the operator and wait for timers in executing to finish
		// 3. when the second step is finished, send a close mail to ensure that the processing-time timers
		//    yet in mailbox are finished before exiting the following mailbox processing loop
		// TODO: To ensure the strict semantics of "close", the operator should be allowed to decide how to
		//  handle (cancel or trigger) the pending timers before being closed, and the second step must be
		//  finished before the first step
		// send an empty mail to trigger the following mailbox-processing loop to exit
		CompletableFuture<Void> closingFuture = deferCloseOperatorToMailbox(containingTask)
			.thenRun(this::cancelProcessingTimeTimers)
			.thenRun(() -> sendClosedMail(closedFuture));

		// run the mailbox processing loop until the closing operation is finished
		while (!(closingFuture.isCompletedExceptionally() || closedFuture.isDone())) {
			mailboxExecutor.yield();
		}

		// expose the exception thrown when closing the operator
		if (closingFuture.isCompletedExceptionally()) {
			closingFuture.get();
		}
	}

	private void quiesceProcessingTimeService() {
		if (processingTimeService != null) {
			processingTimeService.quiesce();
		}
	}

	private CompletableFuture<Void> deferCloseOperatorToMailbox(StreamTask<?, ?> containingTask) {
		CompletableFuture<Void> closeOperatorFuture = new CompletableFuture<>();

		mailboxExecutor.execute(
			() -> {
				try {
					closeOperator(containingTask);
					closeOperatorFuture.complete(null);
				} catch (Throwable t) {
					closeOperatorFuture.completeExceptionally(t);
				}
			},
			"StreamOperatorWrapper#closeOperator for " + wrapped
		);
		return closeOperatorFuture;
	}

	private void cancelProcessingTimeTimers() throws CompletionException {
		if (processingTimeService == null) {
			return;
		}

		try {
			processingTimeService.cancelTimersGracefullyAfterQuiesce().get();
		} catch (Throwable throwable) {
			throw new CompletionException(throwable);
		}
	}

	private void sendClosedMail(CompletableFuture<Void> closedFuture) {
		mailboxExecutor.execute(
			() -> closedFuture.complete(null), "emptyMail in StreamOperatorWrapper#closeGracefully for " + wrapped);
	}

	private void closeOperator(StreamTask<?, ?> containingTask) throws Exception {
		synchronized (containingTask.getCheckpointLock()) {
			if (wrapped != null) {
				wrapped.close();
			}
		}
	}

	static class ReadIterator implements Iterator<StreamOperatorWrapper<?, ?>> {

		private StreamOperatorWrapper<?, ?> next;

		ReadIterator(StreamOperatorWrapper<?, ?> first) {
			this.next = first;
		}

		@Override
		public boolean hasNext() {
			return this.next != null;
		}

		@Override
		public StreamOperatorWrapper<?, ?> next() {
			if (hasNext()) {
				StreamOperatorWrapper<?, ?> current = next;
				this.next = this.next.next;
				return current;
			}

			throw new NoSuchElementException();
		}
	}

	static class ReverseReadIterator implements Iterator<StreamOperatorWrapper<?, ?>> {

		private StreamOperatorWrapper<?, ?> next;

		ReverseReadIterator(StreamOperatorWrapper<?, ?> first) {
			this.next = first;
		}

		@Override
		public boolean hasNext() {
			return this.next != null;
		}

		@Override
		public StreamOperatorWrapper<?, ?> next() {
			if (hasNext()) {
				StreamOperatorWrapper<?, ?> current = next;
				this.next = this.next.previous;
				return current;
			}

			throw new NoSuchElementException();
		}
	}
}
