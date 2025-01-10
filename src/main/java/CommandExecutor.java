import java.io.*;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class CommandExecutor {
    private static final ExecutorService replicaThreadPool = Executors.newCachedThreadPool();
    public static CountDownLatch latch;
    public static void execute(Command command, DataOutputStream out) {
        boolean isWriteCommand = command.getCommand() == CommandName.SET;
        execute(command, out, false);
        if (isWriteCommand) {
            System.out.println("received write command. Propagating to replicas");
            propagateToReplicas(command);
            // incremeent the master replication offset by the number of bytes in the command
            Main.masterReplOffset += command.getLengthInBytes();
        }
    }

    public static void execute(Command command, DataOutputStream out, boolean isSilent) {
        switch (command.getCommand()) {
            case PING -> {
                executePing(command, out, isSilent);
            }
            case ECHO -> {
                executeEcho(command, out, isSilent);
            }
            case SET -> {
                executeSet(command, out, isSilent);
            }
            case GET -> {
                executeGet(command, out, isSilent);
            }
            case KEYS -> {
                executeKeys(command, out, isSilent);
            }
            case CONFIG -> {
                executeConfig(command, out, isSilent);
            }
            case INFO -> {
                executeInfo(command, out, isSilent);
            }
            case REPLCONF -> {
                executeReplConf(command, out, isSilent);
            }
            case PSYNC -> {
                executePsync(command, out, isSilent);
            }
            case WAIT -> {
                executeWait(command, out, isSilent);
            }
            default -> {
                throw new IllegalArgumentException("Invalid command");
            }
        }
    }

    private static void propagateToReplicas(Command command) {
        if (Main.replicaMap != null) {
            for (DataOutputStream replicaOut : Main.replicaMap.keySet()) {
                // print out the host and port of the replica
                System.out.println("Propagating to replica: " + replicaOut);
                replicaThreadPool.execute(() -> {
                    try {
                        String[] commandStrings = command.toArray();
                        System.out.println("Propagating command to replica: " + commandStrings);
                        writeArray(replicaOut, commandStrings);
                    } catch (Exception e) {
                        System.out.println(Thread.currentThread().getName() + " Failed to propagate command to replica: " + e.getMessage());
                    }
                });
            }
        }
    }

    private static void executePsync(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
        writeSimpleString(out, "FULLRESYNC " + Main.masterReplId + " " + Main.masterReplOffset);
        String emptyFileContents = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] bytes = Base64.getDecoder().decode(emptyFileContents);
        try {
            out.writeBytes("$" + bytes.length + "\r\n");
            out.write(bytes);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }   
    }

    private static void executeReplConf(Command command, DataOutputStream out, boolean isSilent) {
        System.out.println("received REPLCONF command");
        if (command.getArgs()[0].equalsIgnoreCase("getack")) {
            writeArray(out, new String[]{"REPLCONF", "ACK", String.valueOf(CommandParser.totalCommandBytesProcessed - 37)});
        } else {
            executeReplConfOk(command, out, isSilent);
        }
    }

    private static void executeReplConfOk(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
        writeSimpleString(out, "OK");
        try {
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeInfo(Command command, DataOutputStream out, boolean isSilent) {
        StringBuilder info = new StringBuilder();
        if (Main.masterHostAndPort != null) {
            info.append("role:slave\r\n");
        } else {
            info.append("role:master\r\n");
        }

        info.append("master_replid:"+ Main.masterReplId +"\r\n");
        info.append("master_repl_offset:" + Main.masterReplOffset + "\r\n");

        if (!isSilent) {
            writeBulkString(out, info.toString());
        }
    }

    private static void executeKeys(Command command, DataOutputStream out, boolean isSilent) {
        String rdbFilePath = Main.rdbFilePath;
        RdbFileReader rdbFileReader = new RdbFileReader();
        try {
            List<String> keys = rdbFileReader.getKeys(rdbFilePath);
            // write the list of keys as a bulk string array
            if (!isSilent) {
                writeArray(out, keys.toArray(new String[keys.size()]));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeConfig(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
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

    private static void executeGet(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
        System.out.println("received GET command");
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

    private static void executeSet(Command command, DataOutputStream out, boolean isSilent) {
        System.out.println("received SET command");
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        // check if the command provides an expiry time. If it does, the command will be of the form:
        // SET key value px milliseconds
        if (command.getArgs().length == 4) {
            long expiryTime = Long.parseLong(command.getArgs()[3]);
            keyValueStore.put(command.getArgs()[0], command.getArgs()[1], expiryTime);
        } else {
            keyValueStore.put(command.getArgs()[0], command.getArgs()[1]);
        }
        if (!isSilent) {
            try {
                out.writeBytes("+OK\r\n");
                out.flush();
            } catch (IOException e) {
                System.out.println("Failed to write to client " + e.getMessage());
            }
        }
        System.out.println("SET command executed successfully");
    }

    private static void executeEcho(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
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

    public static void writeSimpleString(DataOutputStream out, String arg) {
        try {
            out.writeBytes("+" + arg + "\r\n");
        } catch (IOException e) {
            System.out.println("Failed to write simple string to client " + e.getMessage());
        }
    }

    private static void executePing(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
        try {
            out.writeBytes("+PONG\r\n");
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void executeWait(Command command, DataOutputStream out, boolean isSilent) {
        if (isSilent) return;
        int numReplicas = Integer.parseInt(command.getArgs()[0]);
        int timeout = Integer.parseInt(command.getArgs()[1]);

        // Check if there are pending write operations
        if (Main.masterReplOffset == 0) {
            // No pending write operations, return the number of connected replicas
            try {
                out.writeBytes(":" + Main.replicaMap.size() + "\r\n");
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        latch = new CountDownLatch(numReplicas);
        System.out.println("Current thread executing WAIT: " + Thread.currentThread().getName());
        for (DataOutputStream replicaOut : Main.replicaMap.keySet()) {
            new Thread(() -> {
                try {
                    CommandExecutor.writeArray(replicaOut, new String[]{"REPLCONF", "GETACK", "*"});
                } catch (Exception e) {
                    System.out.println("Failed to send REPLCONF GETACK to replica: " + e.getMessage());
                }
            }).start();
        }

        try {
            boolean completed = latch.await(timeout, TimeUnit.MILLISECONDS);
            System.out.println("countdown completed? " + completed);
            out.writeBytes(":" + (completed ? numReplicas : numReplicas - latch.getCount()) + "\r\n");
            out.flush();
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            // Reset masterReplOffset after processing the WAIT command
            Main.masterReplOffset = 0;
        }
    }

    public static void writeRdbFile(byte[] rdbFileData, String rdbFilePath) {
        try (FileOutputStream fos = new FileOutputStream(rdbFilePath)) {
            fos.write(rdbFileData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
