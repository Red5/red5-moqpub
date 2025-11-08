package org.red5.io.moq.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Variable-length integer encoding/decoding for MOQ Transport protocol.
 * Uses QUIC variable-length integer encoding as defined in RFC 9000 Section 16.
 *
 * Variable-length integers are encoded with a two-bit prefix indicating
 * the number of bytes used:
 * - 00: 1 byte (6-bit value, 0-63)
 * - 01: 2 bytes (14-bit value, 0-16383)
 * - 10: 4 bytes (30-bit value, 0-1073741823)
 * - 11: 8 bytes (62-bit value, 0-4611686018427387903)
 *
 * @see <a href="https://www.rfc-editor.org/rfc/rfc9000.html#section-16">RFC 9000 Section 16</a>
 */
public final class VarInt {

    // Maximum values for each encoding length
    public static final long MAX_1_BYTE = 0x3FL;              // 63
    public static final long MAX_2_BYTE = 0x3FFFL;            // 16383
    public static final long MAX_4_BYTE = 0x3FFFFFFFL;        // 1073741823
    public static final long MAX_8_BYTE = 0x3FFFFFFFFFFFFFFFL; // 4611686018427387903

    private VarInt() {
        // Utility class, prevent instantiation
    }

    /**
     * Write a variable-length integer to a ByteBuf.
     *
     * @param buf   the ByteBuf to write to
     * @param value the value to encode (must be non-negative and <= MAX_8_BYTE)
     * @throws IllegalArgumentException if value is out of range
     */
    public static void write(ByteBuf buf, long value) {
        if (value < 0) {
            throw new IllegalArgumentException("VarInt value must be non-negative: " + value);
        }

        if (value <= MAX_1_BYTE) {
            // 1 byte: 00xxxxxx
            buf.writeByte((int) value);
        } else if (value <= MAX_2_BYTE) {
            // 2 bytes: 01xxxxxx xxxxxxxx
            buf.writeByte((int) ((value >> 8) | 0x40));
            buf.writeByte((int) (value & 0xFF));
        } else if (value <= MAX_4_BYTE) {
            // 4 bytes: 10xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            buf.writeByte((int) ((value >> 24) | 0x80));
            buf.writeByte((int) ((value >> 16) & 0xFF));
            buf.writeByte((int) ((value >> 8) & 0xFF));
            buf.writeByte((int) (value & 0xFF));
        } else if (value <= MAX_8_BYTE) {
            // 8 bytes: 11xxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
            buf.writeByte((int) ((value >> 56) | 0xC0));
            buf.writeByte((int) ((value >> 48) & 0xFF));
            buf.writeByte((int) ((value >> 40) & 0xFF));
            buf.writeByte((int) ((value >> 32) & 0xFF));
            buf.writeByte((int) ((value >> 24) & 0xFF));
            buf.writeByte((int) ((value >> 16) & 0xFF));
            buf.writeByte((int) ((value >> 8) & 0xFF));
            buf.writeByte((int) (value & 0xFF));
        } else {
            throw new IllegalArgumentException("VarInt value too large: " + value);
        }
    }

    /**
     * Read a variable-length integer from a ByteBuf.
     *
     * @param buf the ByteBuf to read from
     * @return the decoded value
     * @throws IllegalArgumentException if the buffer doesn't contain a valid varint
     */
    public static long read(ByteBuf buf) {
        if (!buf.isReadable()) {
            throw new IllegalArgumentException("Buffer is empty");
        }

        int firstByte = buf.readUnsignedByte();
        int prefix = (firstByte >> 6) & 0x03;

        switch (prefix) {
            case 0: // 1 byte
                return firstByte & 0x3F;

            case 1: // 2 bytes
                if (buf.readableBytes() < 1) {
                    throw new IllegalArgumentException("Incomplete 2-byte varint");
                }
                return ((firstByte & 0x3F) << 8) | buf.readUnsignedByte();

            case 2: // 4 bytes
                if (buf.readableBytes() < 3) {
                    throw new IllegalArgumentException("Incomplete 4-byte varint");
                }
                long value4 = (firstByte & 0x3F);
                value4 = (value4 << 8) | buf.readUnsignedByte();
                value4 = (value4 << 8) | buf.readUnsignedByte();
                value4 = (value4 << 8) | buf.readUnsignedByte();
                return value4;

            case 3: // 8 bytes
                if (buf.readableBytes() < 7) {
                    throw new IllegalArgumentException("Incomplete 8-byte varint");
                }
                long value8 = (firstByte & 0x3F);
                value8 = (value8 << 8) | buf.readUnsignedByte();
                value8 = (value8 << 8) | buf.readUnsignedByte();
                value8 = (value8 << 8) | buf.readUnsignedByte();
                value8 = (value8 << 8) | buf.readUnsignedByte();
                value8 = (value8 << 8) | buf.readUnsignedByte();
                value8 = (value8 << 8) | buf.readUnsignedByte();
                value8 = (value8 << 8) | buf.readUnsignedByte();
                return value8;

            default:
                throw new IllegalArgumentException("Invalid varint prefix: " + prefix);
        }
    }

    /**
     * Peek at the next variable-length integer without consuming it.
     *
     * @param buf the ByteBuf to peek from
     * @return the decoded value
     * @throws IllegalArgumentException if the buffer doesn't contain a valid varint
     */
    public static long peek(ByteBuf buf) {
        int readerIndex = buf.readerIndex();
        try {
            return read(buf);
        } finally {
            buf.readerIndex(readerIndex);
        }
    }

    /**
     * Calculate the encoded length of a value in bytes.
     *
     * @param value the value to measure
     * @return the number of bytes needed to encode the value
     */
    public static int length(long value) {
        if (value <= MAX_1_BYTE) {
            return 1;
        } else if (value <= MAX_2_BYTE) {
            return 2;
        } else if (value <= MAX_4_BYTE) {
            return 4;
        } else if (value <= MAX_8_BYTE) {
            return 8;
        } else {
            throw new IllegalArgumentException("VarInt value too large: " + value);
        }
    }

    /**
     * Write a string as length-prefixed UTF-8 bytes.
     * Format: varint(length) + UTF-8 bytes
     *
     * @param buf the ByteBuf to write to
     * @param str the string to encode
     */
    public static void writeString(ByteBuf buf, String str) {
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        write(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    /**
     * Read a length-prefixed string.
     * Format: varint(length) + UTF-8 bytes
     *
     * @param buf the ByteBuf to read from
     * @return the decoded string
     */
    public static String readString(ByteBuf buf) {
        long length = read(buf);
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("String too long: " + length);
        }
        byte[] bytes = new byte[(int) length];
        buf.readBytes(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Write a byte array as length-prefixed data.
     * Format: varint(length) + bytes
     *
     * @param buf   the ByteBuf to write to
     * @param bytes the bytes to encode
     */
    public static void writeBytes(ByteBuf buf, byte[] bytes) {
        write(buf, bytes.length);
        buf.writeBytes(bytes);
    }

    /**
     * Read length-prefixed bytes.
     * Format: varint(length) + bytes
     *
     * @param buf the ByteBuf to read from
     * @return the decoded bytes
     */
    public static byte[] readBytes(ByteBuf buf) {
        long length = read(buf);
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Byte array too long: " + length);
        }
        byte[] bytes = new byte[(int) length];
        buf.readBytes(bytes);
        return bytes;
    }
}
