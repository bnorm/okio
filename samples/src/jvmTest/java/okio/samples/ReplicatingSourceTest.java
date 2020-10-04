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
import okio.ByteString;
import okio.Okio;
import okio.Pipe;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class ReplicatingSourceTest {
  enum BufferFactory {
    EMPTY {
      @Override Buffer newBuffer() {
        return new Buffer();
      }
    },

    SMALL_BUFFER {
      @Override Buffer newBuffer() {
        return new Buffer().writeUtf8("abcde");
      }
    },

//    SMALL_SEGMENTED_BUFFER {
//      @Override Buffer newBuffer() throws Exception {
//        return bufferWithSegments("abc", "defg", "hijkl");
//      }
//    },

    LARGE_BUFFER {
      @Override Buffer newBuffer() {
        Random dice = new Random(0);
        byte[] largeByteArray = new byte[512 * 1024];
        dice.nextBytes(largeByteArray);

        return new Buffer().write(largeByteArray);
      }
//    },
//
//    LARGE_BUFFER_WITH_RANDOM_LAYOUT {
//      @Override Buffer newBuffer() throws Exception {
//        Random dice = new Random(0);
//        byte[] largeByteArray = new byte[512 * 1024];
//        dice.nextBytes(largeByteArray);
//
//        return bufferWithRandomSegmentLayout(dice, largeByteArray);
//      }
    };

    abstract Buffer newBuffer() throws Exception;
  }

  @Parameters(name = "{0}")
  public static List<Object[]> parameters() {
    List<Object[]> result = new ArrayList<>();
    for (BufferFactory bufferFactory : BufferFactory.values()) {
      result.add(new Object[] { bufferFactory });
    }
    return result;
  }

  @Parameter public BufferFactory bufferFactory;

  final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);

  @After public void tearDown() {
    executorService.shutdown();
  }

  @Test public void sink() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    Buffer sink = new Buffer();
    ByteString snapshot = buffer.snapshot();
    ReplicatingSource source = new ReplicatingSource(buffer, sink);

    // Read source completely
    assertEquals(snapshot, Okio.buffer(source).readByteString());

    // Verify source has been exhausted
    assertEquals(0L, buffer.size());

    // Read sink completely
    assertEquals(snapshot, sink.readByteString());
  }

  @Test public void closed() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    assumeTrue("size=" + buffer.size(), buffer.size() > 0);
    ByteString first = ByteString.of(buffer.getByte(0));
    Buffer sink = new Buffer();
    ReplicatingSource source = new ReplicatingSource(buffer, sink);

    // Read the first byte from the primary and close
    Buffer readBuffer = new Buffer();
    source.read(readBuffer, 1);
    source.close();
    assertEquals(first, readBuffer.readByteString());

    // Secondary should only read as much as the primary
    assertEquals(first, sink.readByteString());
  }

  @Test public void stopped() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    ByteString snapshot = buffer.snapshot();
    Buffer sink = new Buffer();
    ReplicatingSource source = new ReplicatingSource(buffer, sink);
    source.stop();

    ByteString primary = Okio.buffer(source).readByteString();
    assertEquals(snapshot, primary);

    ByteString secondary = sink.readByteString();
    assertEquals(0, secondary.size());
  }

  @Test public void stoppedOnClose() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    ByteString snapshot = buffer.snapshot();
    Buffer sink = new Buffer();
    ReplicatingSource source = new ReplicatingSource(buffer, sink);
    source.stopOnClose(sink).close();

    ByteString primary = Okio.buffer(source).readByteString();
    assertEquals(snapshot, primary);

    ByteString secondary = sink.readByteString();
    assertEquals(0, secondary.size());
  }

  @Test public void readBlocked() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    assumeTrue("size=" + buffer.size(), buffer.size() > 0);
    final Pipe pipe = new Pipe(buffer.size() / 2);
    final ReplicatingSource source = new ReplicatingSource(buffer, pipe.sink());
    executorService.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          Okio.buffer(pipe.source()).readByteString();
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
    }, 1000, TimeUnit.MILLISECONDS);

    double start = now();
    Okio.buffer(source).readByteString();
    assertElapsed(1000.0, start);
  }

  @Test public void sinkBlocked() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    final Pipe pipe = new Pipe(Long.MAX_VALUE);
    final ReplicatingSource source = new ReplicatingSource(buffer, pipe.sink());
    executorService.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          Okio.buffer(source).readByteString();
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
    }, 1000, TimeUnit.MILLISECONDS);

    double start = now();
    Okio.buffer(pipe.source()).readByteString();
    assertElapsed(1000.0, start);
  }

  @Test public void replicationTimeout() throws Exception {
    Buffer buffer = bufferFactory.newBuffer();
    assumeTrue("size=" + buffer.size(), buffer.size() > 0);
    final Pipe pipe = new Pipe(buffer.size() / 2);
    final ReplicatingSource source = new ReplicatingSource(buffer, pipe.sink());
    pipe.sink().timeout().timeout(1000, TimeUnit.MILLISECONDS);

    double start = now();
    Buffer readBuffer = new Buffer();
    try {
      source.read(readBuffer, buffer.size());
      fail();
    } catch (InterruptedIOException expected) {
      assertEquals("timeout", expected.getMessage());
    }
    assertElapsed(1000.0, start);
    assertEquals(0, readBuffer.size());
  }

  /** Returns the nanotime in milliseconds as a double for measuring timeouts. */
  private double now() {
    return System.nanoTime() / 1000000.0d;
  }

  /**
   * Fails the test unless the time from start until now is duration, accepting differences in
   * -50..+450 milliseconds.
   */
  private void assertElapsed(double duration, double start) {
    assertEquals(duration, now() - start - 200d, 250.0);
  }
}
