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
            default -> {
                throw new IllegalArgumentException("Invalid command");
            }
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
