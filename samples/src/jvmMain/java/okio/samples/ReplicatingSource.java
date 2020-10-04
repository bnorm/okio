/*
 * Copyright (C) 2020 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package okio.samples;

import okio.Buffer;
import okio.ForwardingSource;
import okio.Pipe;
import okio.Sink;
import okio.Source;

import java.io.IOException;

/**
 * A source that replicates all reads to a sink. All bytes read are copied to the replication sink
 * before being written to the read buffer. This makes sure that all data read by the source is
 * guaranteed to have been replicated. This also means that the replication sink needs to confirm
 * all writes before reads to this source can be confirmed. This also means the sink will receive
 * the read bytes before the user of this source.
 *
 * <p><strong>Closing of the replication sink should be left to this source.</strong> The sink can
 * be closed by either closing or {@link #stop() stopping} the source.
 *
 * <p>To make replication asynchronous, use a {@link Pipe}. Make the sink of the
 * <code>Pipe</code> the sink of this <code>ReplicationSource</code>. The source side of the
 * <code>Pipe</code> should be wrapped with {@link #stopOnClose(Source)} so the replication is
 * automatically stopped when the source of the pipe is closed.
 *
 * <pre>{@code
 * Source source = ...
 * Pipe pipe = new Pipe(Long.MAX_VALUE);
 * ReplicatingSource primary = new ReplicatingSource(source, pipe.sink());
 * Source secondary = primary.stopOnClose(pipe.source());
 * }</pre>
 */
public final class ReplicatingSource extends ForwardingSource {

  /** Used to cache reads until replication is confirmed. */
  private final Buffer readBuffer = new Buffer();

  /** Used to copy reads for writing to the replication sink. */
  private final Buffer replBuffer = new Buffer();

  private final Sink replication;

  /** State of the replication link. */
  private boolean stopped;

  public ReplicatingSource(Source source, Sink replication) {
    super(source);
    this.replication = replication;
  }

  @Override
  public long read(Buffer sink, long byteCount) throws IOException {
    // Read from source into read buffer.
    long result = super.read(readBuffer, byteCount);
    if (result == -1) {
      stop(); // stop replication
      return result;
    }

    synchronized (replication) {
      if (!stopped) {
        // Copy read buffer and write to replication
        readBuffer.copyTo(replBuffer, 0, result);
        replication.write(replBuffer, result);
      }
    }

    // Confirm read by writing read buffer to read sink
    sink.write(readBuffer, result);
    return result;
  }

  @Override
  public void close() throws IOException {
    stop(); // stop replication
    super.close();
  }

  /** Stop replicating data.  Non-reversible. */
  public void stop() throws IOException {
    synchronized (replication) {
      stopped = true;
      replication.close();
    }
  }

  /** Returns a wrapped source which will stop replication when closed. */
  public Source stopOnClose(Source source) {
    return new ForwardingSource(source) {
      @Override
      public void close() throws IOException {
        stop();
        super.close();
      }
    };
  }
}
