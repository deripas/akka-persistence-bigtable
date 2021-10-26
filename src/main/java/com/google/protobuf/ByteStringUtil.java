package com.google.protobuf;

import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.common.primitives.Longs;
import lombok.experimental.UtilityClass;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@UtilityClass
public class ByteStringUtil {

    public static ByteString wrap(byte[] bytes) {
        return ByteString.wrap(bytes);
    }

    public static ByteString prefix(String prefix, byte delimiter) {
        ByteBuffer buffer = createBufferWithPrefix(prefix, delimiter, 0);
        buffer.rewind();
        return ByteString.wrap(buffer);
    }

    public static ByteString key(String prefix, byte delimiter, long sequenceNr) {
        ByteBuffer buffer = createBufferWithPrefix(prefix, delimiter, Long.BYTES);
        buffer.asLongBuffer().put(sequenceNr);
        buffer.rewind();
        return ByteString.wrap(buffer);
    }

    public static long sequenceNr(ByteString key) {
        ByteString bytes = key.substring(key.size() - Long.BYTES);
        return bytes.asReadOnlyByteBuffer()
                .asLongBuffer()
                .get();
    }

    public static Range.ByteStringRange range(String prefix, byte delimiter, long fromSequenceNr, long toSequenceNr) {
        ByteString from = key(prefix, delimiter, fromSequenceNr);
        ByteString to = key(prefix, delimiter, toSequenceNr);
        return range(from, to);
    }

    public static Range.ByteStringRange range(ByteString from, ByteString to) {
        return Range.ByteStringRange.unbounded().startClosed(from).endClosed(to);
    }

    private static ByteBuffer createBufferWithPrefix(String prefix, byte delimiter, int suffixSize) {
        byte[] id = prefix.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.wrap(new byte[id.length + 1 + suffixSize]);
        buffer.put(id);
        buffer.put(delimiter);
        return buffer;
    }
}
