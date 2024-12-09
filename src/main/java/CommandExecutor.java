import java.io.DataOutputStream;
import java.io.IOException;

public class CommandExecutor {
    public static void execute(Command command, DataOutputStream out) {
        switch (command.getCommand()) {
            case PING -> {
                executePing(command, out);
            }
            case ECHO -> {
                executeEcho(command, out);
            }
            case SET -> {
                executeSet(command, out);
            }
            case GET -> {
                executeGet(command, out);
            }
            default -> {
                throw new IllegalArgumentException("Invalid command");
            }
        }
    }

    private static void executeGet(Command command, DataOutputStream out) {
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        String value = (String) keyValueStore.get(command.getArgs()[0]);
        if (value == null) {
            try {
                out.writeBytes("-ERR the key does not exist\r\n");
                out.flush();
            } catch (IOException e) {
                System.out.println("Failed to write to client " + e.getMessage());
            }
        } else {
            writeBulkString(out, value);
        }
    }

    private static void executeSet(Command command, DataOutputStream out) {
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        keyValueStore.put(command.getArgs()[0], command.getArgs()[1]);
        try {
            out.writeBytes("+OK\r\n");
            out.flush();
        } catch (IOException e) {
            System.out.println("Failed to write to client " + e.getMessage());
        }
    }

    private static void executeEcho(Command command, DataOutputStream out) {
        writeBulkString(out, command.getArgs()[0]);
    }

    private static void writeBulkString(DataOutputStream out, String arg) {
        try {
            out.writeBytes("$" + arg.length() + "\r\n");
            out.writeBytes(arg + "\r\n");
            out.flush();
        } catch (IOException e) {
            System.out.println("Failed to write bulk string to client " + e.getMessage());
        }
    }

    private static void executePing(Command command, DataOutputStream out) {
        try {
            out.writeBytes("+PONG\r\n");
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
