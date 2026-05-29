// Copyright 2019 Zethian Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Serilog.Debugging;
using Serilog.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Serilog.Sinks.Batch
{
    internal abstract class BatchProvider : IDisposable
    {
        private const int MaxSupportedBufferSize = 100_000;
        private const int MaxSupportedBatchSize = 1_000;
        private int _numMessages;
        private int _droppedCount;
        private bool _canStop;
        private readonly int _maxBufferSize;
        private readonly int _batchSize;
        private readonly ConcurrentQueue<LogEvent> _logEventBatch;
        private readonly BlockingCollection<IList<LogEvent>> _batchEventsCollection;
        private readonly BlockingCollection<LogEvent> _eventsCollection;
        private readonly TimeSpan _timerThresholdSpan = TimeSpan.FromSeconds(10);
        private readonly TimeSpan _transientThresholdSpan = TimeSpan.FromSeconds(5);
        private readonly Task _timerTask;
        private readonly Task _batchTask;
        private readonly Task _eventPumpTask;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly AutoResetEvent _timerResetEvent = new AutoResetEvent(false);
        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        protected BatchProvider(int batchSize = 100, int maxBufferSize = 25_000)
        {
            _maxBufferSize = Math.Min(Math.Max(5_000, maxBufferSize), MaxSupportedBufferSize);
            _batchSize     = Math.Min(Math.Max(batchSize, 1), MaxSupportedBatchSize);

            _logEventBatch         = new ConcurrentQueue<LogEvent>();
            _batchEventsCollection = new BlockingCollection<IList<LogEvent>>();
            _eventsCollection      = new BlockingCollection<LogEvent>(maxBufferSize);

            // Unwrap() is required: StartNew(asyncMethod) returns Task<Task>.
            // Without Unwrap(), Wait() on _batchTask returns the moment PumpAsync
            // first yields — before any writes have happened.
            _batchTask     = Task.Factory.StartNew(PumpAsync, TaskCreationOptions.LongRunning).Unwrap();
            _timerTask     = Task.Factory.StartNew(TimerPump, TaskCreationOptions.LongRunning);
            _eventPumpTask = Task.Factory.StartNew(EventPump, TaskCreationOptions.LongRunning);
        }

        private async Task PumpAsync()
        {
            try {
                while (!_batchEventsCollection.IsCompleted) {
                    var logEvents = _batchEventsCollection.Take(_cancellationTokenSource.Token);
                    SelfLog.WriteLine($"Sending batch of {logEvents.Count} logs");

                    var retValue = await WriteLogEventAsync(logEvents).ConfigureAwait(false);
                    if (retValue) {
                        Interlocked.Add(ref _numMessages, -1 * logEvents.Count);
                    }
                    else {
                        SelfLog.WriteLine($"Retrying after {_transientThresholdSpan.TotalSeconds} seconds...");

                        await Task.Delay(_transientThresholdSpan).ConfigureAwait(false);

                        if (!_batchEventsCollection.IsAddingCompleted) {
                            _batchEventsCollection.Add(logEvents);
                        }
                    }

                    if (_cancellationTokenSource.IsCancellationRequested) {
                        _cancellationTokenSource.Token.ThrowIfCancellationRequested();
                    }
                }
            }
            catch (InvalidOperationException) { }
            catch (OperationCanceledException) { }
            catch (Exception ex) {
                SelfLog.WriteLine(ex.Message);
            }
        }

        private void TimerPump()
        {
            while (!_canStop) {
                _timerResetEvent.WaitOne(_timerThresholdSpan);
                FlushLogEventBatch();
            }
        }

        private void EventPump()
        {
            try {
                while (!_eventsCollection.IsCompleted) {
                    var logEvent = _eventsCollection.Take(_cancellationTokenSource.Token);
                    _logEventBatch.Enqueue(logEvent);

                    if (_logEventBatch.Count >= _batchSize) {
                        FlushLogEventBatch();
                    }
                }
            }
            catch (InvalidOperationException) { }
            catch (OperationCanceledException) { }
            catch (Exception ex) {
                SelfLog.WriteLine(ex.Message);
            }
        }

        private void FlushLogEventBatch()
        {
            var acquired = false;
            try {
                _semaphoreSlim.Wait(_cancellationTokenSource.Token);
                acquired = true;

                if (_logEventBatch.IsEmpty) {
                    return;
                }

                var logEventBatchSize = _logEventBatch.Count >= _batchSize ? _batchSize : _logEventBatch.Count;
                var logEventList = new List<LogEvent>(logEventBatchSize);

                for (var i = 0; i < logEventBatchSize; i++) {
                    if (_logEventBatch.TryDequeue(out LogEvent logEvent)) {
                        logEventList.Add(logEvent);
                    }
                }

                if (!_batchEventsCollection.IsAddingCompleted) {
                    _batchEventsCollection.Add(logEventList);
                }
            }
            catch (InvalidOperationException) { }
            catch (OperationCanceledException) { }
            finally {
                if (acquired) {
                    _semaphoreSlim.Release();
                }
            }
        }

        protected void PushEvent(LogEvent logEvent)
        {
            if (_numMessages > _maxBufferSize) {
                var dropped = Interlocked.Increment(ref _droppedCount);
                if (dropped == 1 || dropped % 1000 == 0)
                    SelfLog.WriteLine($"Buffer full ({_maxBufferSize}); {dropped} events dropped so far.");
                return;
            }

            if (_eventsCollection.IsAddingCompleted)
                return;

            _eventsCollection.Add(logEvent);
            Interlocked.Increment(ref _numMessages);
        }

        protected abstract Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch);

        #region IDisposable Support

        private bool _disposedValue; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (_disposedValue)
                return;

            if (disposing) {
                FlushAndCloseEventHandlers();
                _semaphoreSlim.Dispose();

                SelfLog.WriteLine("Sink halted successfully.");
            }

            _disposedValue = true;
        }

        private void FlushAndCloseEventHandlers()
        {
            try {
                SelfLog.WriteLine("Halting sink...");

                _canStop = true;
                _timerResetEvent.Set();

                // Signal EventPump to stop accepting new events and let it drain itself.
                // The disposing thread must NOT also drain _eventsCollection — that races
                // with EventPump and can leave items orphaned in _logEventBatch.
                _eventsCollection.CompleteAdding();
                _eventPumpTask.Wait(TimeSpan.FromSeconds(60));

                // EventPump has exited; flush any partial batch it left in _logEventBatch.
                FlushLogEventBatch();

                // Signal PumpAsync to stop and let it drain _batchEventsCollection itself.
                _batchEventsCollection.CompleteAdding();
                _batchTask.Wait(TimeSpan.FromSeconds(60));
                _timerTask.Wait(TimeSpan.FromSeconds(60));

                // Cancel releases any tasks still parked on a blocking wait (no-op otherwise).
                _cancellationTokenSource.Cancel();
            }
            catch (Exception ex) {
                SelfLog.WriteLine(ex.Message);
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion
    }
}
