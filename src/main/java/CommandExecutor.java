import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

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
            case KEYS -> {
                executeKeys(command, out);
            }
            case INFO -> {
                executeInfo(command, out);
            }
            default -> {
                throw new IllegalArgumentException("Invalid command");
            }
        }
    }

    private static void executeInfo(Command command, DataOutputStream out) {
        String[] args = command.getArgs();
        switch (args[0]) {
            case "replication" -> {
                StringBuilder info = new StringBuilder();
                if (Main.masterHostAndPort != null) {
                    info.append("role:slave\r\n");
                } else {
                    info.append("role:master\r\n");
                }

                info.append("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n");
                info.append("master_repl_offset:0\r\n");
                writeBulkString(out, info.toString());
            }
            default -> {
                throw new IllegalArgumentException("Invalid command");
            }
        }
    }

    private static void executeKeys(Command command, DataOutputStream out) {
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        String rdbFilePath = Main.rdbFilePath;
        RdbFileReader rdbFileReader = new RdbFileReader();
        try {
            List<String> keys = rdbFileReader.getKeys(rdbFilePath);
            // write the list of keys as a bulk string array
            writeArray(out, keys.toArray(new String[keys.size()]));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private static void executeConfig(Command command, DataOutputStream out) {
        // if the first argument is "GET", then the command is of the form:
        // CONFIG GET key
        if (command.getArgs()[0].equalsIgnoreCase("GET")) {
            // if the key is "dir", return the directory where the RDB file is stored
            String rdbFileDirectory = Main.rdbFilePath.substring(0, Main.rdbFilePath.lastIndexOf("/"));
            if (rdbFileDirectory == null) {
                try {
                    out.writeBytes("$-1\r\n");
                    out.flush();
                } catch (IOException e) {
                    System.out.println("Failed to write to client " + e.getMessage());
                }
            } else {
                String[] returnArray = new String[2];
                returnArray[0] = command.getArgs()[1];
                returnArray[1] = rdbFileDirectory;
                writeArray(out, returnArray);
            }
        } else {
            throw new IllegalArgumentException("Invalid command");
        }

    }

    public static void writeArray(DataOutputStream out, String[] returnArray) {
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
        if (value == null && Main.rdbFilePath != null) {
            try {
                RdbFileReader rdbFileReader = new RdbFileReader();
                value = rdbFileReader.readValueFromKey(Main.rdbFilePath, command.getArgs()[0]);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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

    public static void writeBulkString(DataOutputStream out, String arg) {
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
