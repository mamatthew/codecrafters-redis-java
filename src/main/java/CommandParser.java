import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommandParser {

    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';
    public static final byte PLUS_BYTE = '+';
    public static final byte MINUS_BYTE = '-';
    public static final byte COLON_BYTE = ':';
    public static final byte CARRIAGE_RETURN_BYTE = '\r';
    public static final byte LINE_FEED_BYTE = '\n';

    public static int totalCommandBytesProcessed = 0;

    public static Command parse(DataInputStream in) throws Exception {
        List<String> args = process(in, new ArrayList<>());
        return new Command(args);
    }

    static List<String> process(DataInputStream in, List<String> args) throws Exception {
        byte b;
        try {
            b = in.readByte();
            totalCommandBytesProcessed += 1; // Add 1 byte for the command type byte
            switch(b) {
                case ASTERISK_BYTE -> {
                    return processBulkStringArray(in, args);
                }
                case DOLLAR_BYTE -> {
                    return processBulkString(in, args);
                }
                case PLUS_BYTE -> {
                    return processSimpleString(in, args);
                }
                default -> {
                    throw new IllegalArgumentException("Invalid command");
                }
            }
        } catch (IOException e) {
            throw new Exception(("Failed to read from input stream: " + e.getMessage()));
        }
    }

    private static List<String> processSimpleString(DataInputStream in, List<String> args) {
        StringBuilder sb = new StringBuilder();
        try {
            byte b;
            while ((b = in.readByte()) != CARRIAGE_RETURN_BYTE) {
                sb.append((char) b);
                totalCommandBytesProcessed += 1; // Add 1 byte for each character
            }
            in.readByte(); // consume the LINE_FEED_BYTE
            totalCommandBytesProcessed += 2; // Add 2 bytes for CRLF
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        args.add(sb.toString());
        return args;
    }

    private static List<String> processBulkString(DataInputStream in, List<String> args) {
        int len;
        try {
            len = readIntCRLF(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] buf = new byte[len];
        try {
            in.readFully(buf);
            in.readByte(); // consume the CARRIAGE_RETURN_BYTE
            in.readByte(); // consume the LINE_FEED_BYTE
            totalCommandBytesProcessed += len + 2; // Add length of bulk string and CRLF
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        args.add(new String(buf));
        return args;
    }

    private static List<String> processBulkStringArray(DataInputStream in, List<String> args) throws Exception {
        int len;
        try {
            len = readIntCRLF(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (int i = 0; i < len; i++) {
            process(in, args);
        }
        return args;
    }

    public static int readIntCRLF(DataInputStream in) throws IOException {
        int result = 0;
        boolean isNegative = false;
        while (true) {
            byte b = in.readByte();
            totalCommandBytesProcessed += 1; // Add 1 byte for each character
            if (b == MINUS_BYTE) {
                isNegative = true;
            } else if (b == CARRIAGE_RETURN_BYTE) {
                in.readByte(); // consume the line feed
                totalCommandBytesProcessed += 1; // Add 1 byte for LF
                break;
            } else {
                result = result * 10 + (b - '0');
            }
        }
        return isNegative ? -result : result;
    }
}
