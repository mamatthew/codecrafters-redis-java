import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class RdbFileReader {
    private static final byte[] HEADER_MAGIC = "REDIS".getBytes();
    private static final byte[] HEADER_VERSION = "0011".getBytes();

    public List<RdbEntry> getEntries(String filePath) throws IOException {
        List<RdbEntry> entries = new ArrayList<>();
        try (PushbackInputStream in = new PushbackInputStream(new DataInputStream(new FileInputStream(filePath)))) {
            parseHeader(in);
            parseMetadata(in);
            parseDatabase(in, entries);
            parseEndOfFile(in);
        }
        return entries;
    }

    public String readValueFromKey(String filePath, String key) throws IOException {
        try (PushbackInputStream in = new PushbackInputStream(new DataInputStream(new FileInputStream(filePath))) ) {
            parseHeader(in);
            parseMetadata(in);
            return readValueFromKey(in, key);
        }
    }

    private String readValueFromKey(PushbackInputStream in, String key) throws IOException {
        while (true) {
            byte marker = (byte) in.read();
            if (marker == (byte) 0xFF) {
                break;
            }
            if (marker != (byte) 0xFE) {
                throw new RuntimeException("Invalid RDB file: unexpected database marker: " + marker);
            }
            try {
                int index = (byte) in.read(); // read database index
            } catch (IOException e) {
                throw new RuntimeException("Invalid RDB file: failed to parse database index", e);
            }
            int size = 0;
            try {
                size = parseHashTableSize(in);
            } catch (IOException e) {
                throw new RuntimeException("Invalid RDB file: failed to parse hash table size: " + e);
            }
            // parse hash table
            try {
                for (int i = 0; i < size; i++) {
                    byte type = (byte) in.read();
                    boolean isExpired = false;
                    if (type == (byte) 0xFC || type == (byte) 0xFD) {
                        isExpired = parseExpire(in, type) < System.currentTimeMillis();
                        type = (byte) in.read();
                    }
                    String currentKey = parseString(in);
                    String value = parseValue(in, type);
                    if (currentKey.equals(key) && !isExpired) {
                        return value;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Invalid RDB file: failed to parse hash table entry: " + e);
            }
        }
        return null;
    }

    private void parseHeader(PushbackInputStream in) throws IOException {
        byte[] magic = new byte[HEADER_MAGIC.length];
        in.read(magic, 0, HEADER_MAGIC.length);
        if (!new String(magic).equals("REDIS")) {
            throw new IOException("Invalid RDB file: missing magic string");
        }

        byte[] version = new byte[HEADER_VERSION.length];
        in.read(version, 0, HEADER_VERSION.length);
        if (!new String(version).equals("0011")) {
            throw new IOException("Invalid RDB file: unsupported version");
        }
    }

    private void parseMetadata(PushbackInputStream in) throws IOException {
        while (true) {
            byte marker = (byte) in.read();
            if (marker == (byte) 0xFE || marker == (byte) 0xFF) {
                in.unread(marker);
                break;
            }
            if (marker != (byte) 0xFA) {
                System.out.println("Invalid RDB file: unexpected metadata marker: " + marker);
                throw new IOException("Invalid RDB file: unexpected metadata marker");
            }
            String name = parseString(in); // metadata name
            System.out.println("Metadata name: " + name);
            String value = parseString(in); // metadata value
            System.out.println("Metadata value: " + value);
        }
    }

    private void parseDatabase(PushbackInputStream in, List<RdbEntry> entries) throws IOException {
        while (true) {
            byte marker = (byte) in.read();
            if (marker == (byte) 0xFF) {
                break;
            }
            if (marker != (byte) 0xFE) {
                throw new IOException("Invalid RDB file: unexpected database marker: " + marker);
            }
            try {
                int index = (byte) in.read(); // read database index
                System.out.println("Database index: " + index);
            } catch (IOException e) {
                throw new RuntimeException("Invalid RDB file: failed to parse database index", e);
            }

            try {
                int size = parseHashTableSize(in);
                System.out.println("Hash table size: " + size);
                // parse hash table
                for (int i = 0; i < size; i++) {
                    byte type = (byte) in.read();
                    long expiryTime = 0;
                    if (type == (byte) 0xFC || type == (byte) 0xFD) {
                        expiryTime = parseExpire(in, type);
                        type = (byte) in.read();
                    }
                    String key = parseString(in);
                    String value = parseValue(in, type);
                    entries.add(new RdbEntry(key, value, expiryTime));
                }
            } catch (IOException e) {
                throw new RuntimeException("Invalid RDB file: failed to parse hash table size", e);
            }
        }
    }

    private int parseHashTableSize(PushbackInputStream in) throws IOException {
        byte marker = (byte) in.read();
        if (marker != (byte) 0xFB) {
            throw new IOException("Invalid RDB file: unexpected hash table size marker");
        }
        int keyValueSize = parseSize(in); // total key-value hash table size
        System.out.println("Key-value hash table size: " + keyValueSize);
        int keyValueWithExpirySize = parseSize(in); // number of keys with an expiry
        System.out.println("Key-value hash table size with expiry: " + keyValueWithExpirySize);
        return keyValueSize;
    }

    private void parseEndOfFile(PushbackInputStream in) throws IOException {
        // read 8 bytes of checksum
        byte[] checksum = new byte[8];
        in.read(checksum, 0, 8);

    }

    private long parseExpire(PushbackInputStream in, byte type) throws IOException {
        if (type == (byte) 0xFC) {
            byte[] expireTimestamp = new byte[8];
            in.read(expireTimestamp, 0, 8); // expire timestamp in milliseconds
            return ByteBuffer.wrap(expireTimestamp).order(ByteOrder.LITTLE_ENDIAN).getLong();
        } else {
            byte[] expireTimestamp = new byte[4];
            in.read(expireTimestamp, 0, 4); // expire timestamp in seconds
            return ByteBuffer.wrap(expireTimestamp).order(ByteOrder.LITTLE_ENDIAN).getInt() * 1000L;
        }
    }

    private void parseKeyValue(PushbackInputStream in, List<String> keys, byte valueType) throws IOException {
        String key = parseString(in);
        parseValue(in, valueType);
        keys.add(key);
    }

    private String parseValue(PushbackInputStream in, byte valueType) throws IOException {
        switch (valueType) {
            // string
            case 0x00 -> {
                return parseString(in);
            }
            // Add cases for other value types if needed
            default -> throw new IOException("Invalid RDB file: unknown value type");
        }
    }

    private String parseString(PushbackInputStream in) throws IOException {
        byte firstByte = (byte) in.read();
        int length;
        if ((firstByte & 0xC0) == 0x00) {
            // 6-bit encoding
            length = firstByte & 0x3F;
            // 14-bit encoding
        } else if ((firstByte & 0xC0) == 0x40) {
            length = ((firstByte & 0x3F) << 8) | (in.read() & 0xFF);
            // 32-bit encoding
        } else if ((firstByte & 0xC0) == 0x80) {
            byte[] buf = new byte[4];
            length = in.read(buf, 0, 4);
            // string encoding
        } else if ((firstByte & 0xC0) == 0xC0) {
            switch (firstByte & 0x3F) {
                case 0x00: // 8-bit integer
                    byte byteLength = (byte) in.read();
                    return Byte.toString(byteLength);
                case 0x01: // 16-bit integer
                    short shortLength = readLittleEndianShort(in);
                    return Short.toString(shortLength);
                case 0x02: // 32-bit integer
                    int intLength = readLittleEndianInt(in);
                    return Integer.toString(intLength);
                case 0x03: // 64-bit integer
                    throw new IOException("LFZ integer encoding not supported");
                default:
                    throw new IOException("unknown string encoding: " + firstByte);
            }
        } else {
            throw new IOException("Invalid RDB file: unknown size encoding");
        }
        byte[] data = new byte[length];
        in.read(data);
        return new String(data);
    }

    private short readLittleEndianShort(PushbackInputStream in) throws IOException {
        byte[] bytes = new byte[4];
        int bytesRead = in.read(bytes);
        if (bytesRead != 4) {
            throw new RuntimeException("Failed to read 2 bytes for short");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer.getShort();
    }

    private int readLittleEndianInt(PushbackInputStream in) throws IOException {
        byte[] bytes = new byte[4];
        int bytesRead = in.read(bytes);
        if (bytesRead != 4) {
            throw new RuntimeException("Failed to read 4 bytes for int");
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer.getInt();
    }

    private int parseSize(PushbackInputStream in) {
        try {
            byte firstByte = (byte) in.read();
            int length;
            if ((firstByte & 0xC0) == 0x00) {
                length = firstByte & 0x3F;
            } else if ((firstByte & 0xC0) == 0x40) {
                length = ((firstByte & 0x3F) << 8) | (in.read() & 0xFF);
            } else if ((firstByte & 0xC0) == 0x80) {
                byte[] bytes = new byte[4];
                length = in.read(bytes);
            } else if ((firstByte & 0xC0) == 0xC0) {
                switch (firstByte & 0x3F) {
                    case 0x00: // 8-bit integer
                        length = in.read();
                    case 0x01: // 16-bit integer
                        byte[] shortBytes = new byte[2];
                        length = in.read(shortBytes);
                    case 0x02: // 32-bit integer
                        byte[] intBytes = new byte[4];
                        length = in.read(intBytes);
                    default:
                        throw new IOException("Invalid RDB file: unknown string encoding");
                }
            } else {
                throw new IOException("Invalid RDB file: unknown size encoding");
            }
            return length;
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse size: " + e.getMessage());
        }

    }
}

// RdbEntry class to hold key, value, and expiry time
class RdbEntry {
    private final String key;
    private final String value;
    private final long expiryTime;

    public RdbEntry(String key, String value, long expiryTime) {
        this.key = key;
        this.value = value;
        this.expiryTime = expiryTime;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getExpiryTime() {
        return expiryTime;
    }
}