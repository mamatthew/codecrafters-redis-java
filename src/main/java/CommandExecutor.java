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
            case CONFIG -> {
                executeConfig(command, out);
            }
            default -> {
                throw new IllegalArgumentException("Invalid command");
            }
        }
    }

    private static void executeConfig(Command command, DataOutputStream out) {
        // if the first argument is "GET", then the command is of the form:
        // CONFIG GET key
        if (command.getArgs()[0].equalsIgnoreCase("GET")) {
            KeyValueStore keyValueStore = KeyValueStore.getInstance();
            String value = (String) keyValueStore.get(command.getArgs()[1]);
            if (value == null) {
                try {
                    out.writeBytes("$-1\r\n");
                    out.flush();
                } catch (IOException e) {
                    System.out.println("Failed to write to client " + e.getMessage());
                }
            } else {
                String[] returnArray = new String[2];
                returnArray[0] = command.getArgs()[1];
                returnArray[1] = value;
                writeArray(out, returnArray);
            }
        } else {
            throw new IllegalArgumentException("Invalid command");
        }

    }

    private static void writeArray(DataOutputStream out, String[] returnArray) {
        try {
            out.writeBytes("*" + returnArray.length + "\r\n");
            for (String s : returnArray) {
                writeBulkString(out, s);
            }
            out.flush();
        } catch (IOException e) {
            System.out.println("Failed to write array to client " + e.getMessage());
        }
    }

    private static void executeGet(Command command, DataOutputStream out) {
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        String value = (String) keyValueStore.get(command.getArgs()[0]);
        if (value == null) {
            try {
                out.writeBytes("$-1\r\n");
                out.flush();
            } catch (IOException e) {
                System.out.println("Failed to write to client " + e.getMessage());
            }
        } else {
            writeBulkString(out, value);
            try {
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void executeSet(Command command, DataOutputStream out) {
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        // check if the command provides an expiry time. If it does, the command will be of the form:
        // SET key value px milliseconds
        if (command.getArgs().length == 4) {
            long expiryTime = Long.parseLong(command.getArgs()[3]);
            keyValueStore.put(command.getArgs()[0], command.getArgs()[1], expiryTime);
        } else {
            keyValueStore.put(command.getArgs()[0], command.getArgs()[1]);
        }
        try {
            out.writeBytes("+OK\r\n");
            out.flush();
        } catch (IOException e) {
            System.out.println("Failed to write to client " + e.getMessage());
        }
    }

    private static void executeEcho(Command command, DataOutputStream out) {
        writeBulkString(out, command.getArgs()[0]);
        try {
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeBulkString(DataOutputStream out, String arg) {
        try {
            out.writeBytes("$" + arg.length() + "\r\n");
            out.writeBytes(arg + "\r\n");
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
