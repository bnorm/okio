/*
 * Copyright (C) 2019 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO move to RealBufferedSink class: https://youtrack.jetbrains.com/issue/KT-20427
@file:Suppress("NOTHING_TO_INLINE")

package okio.internal

import okio.Buffer
import okio.BufferedSink
import okio.ByteString
import okio.EOFException
import okio.RealBufferedSink
import okio.Segment
import okio.Source

private inline fun RealBufferedSink.emitCompleteSegments(block: () -> Unit): BufferedSink {
  check(!closed) { "closed" }
  block()
  val byteCount = buffer.completeSegmentByteCount()
  if (byteCount > 0L) sink.write(buffer, byteCount)
  return this
}

internal inline fun RealBufferedSink.commonWrite(source: Buffer, byteCount: Long) {
  emitCompleteSegments { buffer.write(source, byteCount) }
}

internal inline fun RealBufferedSink.commonWrite(byteString: ByteString) =
  emitCompleteSegments { buffer.write(byteString) }

internal inline fun RealBufferedSink.commonWrite(byteString: ByteString, offset: Int, byteCount: Int) =
  emitCompleteSegments { buffer.write(byteString, offset, byteCount) }

internal inline fun RealBufferedSink.commonWriteUtf8(string: String) =
  emitCompleteSegments { buffer.writeUtf8(string) }

internal inline fun RealBufferedSink.commonWriteUtf8(string: String, beginIndex: Int, endIndex: Int) =
  emitCompleteSegments { buffer.writeUtf8(string, beginIndex, endIndex) }

internal inline fun RealBufferedSink.commonWriteUtf8CodePoint(codePoint: Int) =
  emitCompleteSegments { buffer.writeUtf8CodePoint(codePoint) }

internal inline fun RealBufferedSink.commonWrite(source: ByteArray) =
  emitCompleteSegments { buffer.write(source) }

internal inline fun RealBufferedSink.commonWrite(source: ByteArray, offset: Int, byteCount: Int) =
  emitCompleteSegments { buffer.write(source, offset, byteCount) }

internal inline fun RealBufferedSink.commonWriteAll(source: Source): Long {
  var totalBytesRead = 0L
  while (true) {
    val readCount: Long = source.read(buffer, Segment.SIZE.toLong())
    if (readCount == -1L) break
    totalBytesRead += readCount
    emitCompleteSegments()
  }
  return totalBytesRead
}

internal inline fun RealBufferedSink.commonWrite(source: Source, byteCount: Long): BufferedSink {
  var remaining = byteCount
  while (remaining > 0L) {
    val read = source.read(buffer, remaining)
    if (read == -1L) throw EOFException()
    remaining -= read
    emitCompleteSegments()
  }
  return this
}

internal inline fun RealBufferedSink.commonWriteByte(b: Int) =
  emitCompleteSegments { buffer.writeByte(b) }

internal inline fun RealBufferedSink.commonWriteShort(s: Int) =
  emitCompleteSegments { buffer.writeShort(s) }

internal inline fun RealBufferedSink.commonWriteShortLe(s: Int) =
  emitCompleteSegments { buffer.writeShortLe(s) }

internal inline fun RealBufferedSink.commonWriteInt(i: Int) =
  emitCompleteSegments { buffer.writeInt(i) }

internal inline fun RealBufferedSink.commonWriteIntLe(i: Int) =
  emitCompleteSegments { buffer.writeIntLe(i) }

internal inline fun RealBufferedSink.commonWriteLong(v: Long) =
  emitCompleteSegments { buffer.writeLong(v) }

internal inline fun RealBufferedSink.commonWriteLongLe(v: Long) =
  emitCompleteSegments { buffer.writeLongLe(v) }

internal inline fun RealBufferedSink.commonWriteDecimalLong(v: Long) =
  emitCompleteSegments { buffer.writeDecimalLong(v) }

internal inline fun RealBufferedSink.commonWriteHexadecimalUnsignedLong(v: Long) =
  emitCompleteSegments { buffer.writeHexadecimalUnsignedLong(v) }

internal inline fun RealBufferedSink.commonEmitCompleteSegments() =
  emitCompleteSegments { /* do nothing */ }

internal inline fun RealBufferedSink.commonEmit(): BufferedSink {
  check(!closed) { "closed" }
  val byteCount = buffer.size
  if (byteCount > 0L) sink.write(buffer, byteCount)
  return this
}

internal inline fun RealBufferedSink.commonFlush() {
  check(!closed) { "closed" }
  if (buffer.size > 0L) {
    sink.write(buffer, buffer.size)
  }
  sink.flush()
}

internal inline fun RealBufferedSink.commonClose() {
  if (closed) return

  // Emit buffered data to the underlying sink. If this fails, we still need
  // to close the sink; otherwise we risk leaking resources.
  var thrown: Throwable? = null
  try {
    if (buffer.size > 0) {
      sink.write(buffer, buffer.size)
    }
  } catch (e: Throwable) {
    thrown = e
  }

  try {
    sink.close()
  } catch (e: Throwable) {
    if (thrown == null) thrown = e
  }

  closed = true

  if (thrown != null) throw thrown
}

internal inline fun RealBufferedSink.commonTimeout() = sink.timeout()

internal inline fun RealBufferedSink.commonToString() = "buffer($sink)"
