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

    public static Command parse(DataInputStream in) throws Exception {
        return new Command(process(in, new ArrayList<>()));
    }

    private static List<String> process(DataInputStream in, List<String> args) throws Exception {
        byte b;
        try {
            b = in.readByte();
            switch(b) {
                case ASTERISK_BYTE -> {
                    return processBulkStringArray(in, args);
                }
                case DOLLAR_BYTE -> {
                    return processBulkString(in, args);
                }
                default -> {
                    throw new IllegalArgumentException("Invalid command");
                }
            }
        } catch (IOException e) {
            throw new Exception(("Failed to read from input stream: " + e.getMessage()));
        }
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

    private static int readIntCRLF(DataInputStream in) throws IOException {
        int result = 0;
        boolean isNegative = false;
        while (true) {
            byte b = in.readByte();
            if (b == MINUS_BYTE) {
                isNegative = true;
            } else if (b == CARRIAGE_RETURN_BYTE) {
                in.readByte(); // consume the line feed
                break;
            } else {
                result = result * 10 + (b - '0');
            }
        }
        return isNegative ? -result : result;
    }
}
