import com.beust.jcommander.JCommander;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Base64;
import java.io.FileOutputStream;
import java.io.File;

public class Main {
    private static final int THREAD_POOL_SIZE = 10;
    public static String rdbFilePath = null;
    private static int port;
    static String masterHostAndPort;
    public static String masterReplId;
    public static int masterReplOffset;
    public static DataInputStream masterInputStream;
    public static DataOutputStream masterOutputStream;
    public static ConcurrentHashMap<DataOutputStream, DataInputStream> replicaMap = new ConcurrentHashMap<>();

    public static void main(String[] args){
          setup(args);
          ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
          System.out.println("Starting server on port " + port);
          if (masterHostAndPort != null) {
              threadPool.execute(() -> listenToMaster());
          }
          try (ServerSocket serverSocket = new ServerSocket(port)) {
              serverSocket.setReuseAddress(true);
              while (true) {
                  // Wait for connection from client.
                  Socket clientSocket = serverSocket.accept();
                  System.out.println("Accepted connection from client");
                  threadPool.execute(new ClientHandler(clientSocket));
              }

            } catch (IOException e) {
              System.out.println("IOException: " + e.getMessage());
            } finally {
              threadPool.shutdown();
            }
    }

    private static void listenToMaster() {
        System.out.println("Listening to master");
        while (true) {
            try {
                Command command = CommandParser.parse(masterInputStream);
                System.out.println("Received command from master: " + command.getCommand());
                CommandExecutor.execute(command, masterOutputStream, true);
            } catch (Exception e) {
                System.out.println("Failed to read from master: " + e.getMessage());
                break;
            }
        }
    }

    private static void setup(String[] args) {
        CommandLineArgs commandLineArgs = new CommandLineArgs();
        JCommander.newBuilder()
                .addObject(commandLineArgs)
                .build()
                .parse(args);

        if (commandLineArgs.dir != null && commandLineArgs.dbfilename != null) {
            rdbFilePath = commandLineArgs.dir + "/" + commandLineArgs.dbfilename;
        } else {
            rdbFilePath = "persistence/dump.rdb";
            createDefaultRdbFile(rdbFilePath);
        }
        port = commandLineArgs.port;
        masterHostAndPort = commandLineArgs.replicaof;
        if (masterHostAndPort != null) {
            String host = masterHostAndPort.split(" ")[0];
            int port = Integer.parseInt(masterHostAndPort.split(" ")[1]);
            sendHandshakeToMaster(host, port);
        } else {
            replicaMap = new ConcurrentHashMap<>();
            masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
            masterReplOffset = 0;
        }

        loadRdbFileIntoKeyValueStore(rdbFilePath);
    }

    private static void createDefaultRdbFile(String rdbFilePath) {
        String base64Content = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        byte[] decodedBytes = Base64.getDecoder().decode(base64Content);
        File rdbFile = new File(rdbFilePath);
        rdbFile.getParentFile().mkdirs(); // Ensure the directory exists
        try (FileOutputStream fos = new FileOutputStream(rdbFile)) {
            fos.write(decodedBytes);
            System.out.println("Default RDB file created at " + rdbFilePath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create default RDB file: " + e.getMessage(), e);
        }
    }

    private static void loadRdbFileIntoKeyValueStore(String rdbFilePath) {
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        RdbFileReader rdbFileReader = new RdbFileReader();
        try {
            List<RdbEntry> entries = rdbFileReader.getEntries(rdbFilePath);
            for (RdbEntry entry : entries) {
                if (entry.getExpiryTime() > 0) {
                    keyValueStore.put(entry.getKey(), entry.getValue(), entry.getExpiryTime() - System.currentTimeMillis());
                } else {
                    keyValueStore.put(entry.getKey(), entry.getValue());
                }
            }
            System.out.println("RDB file loaded into KeyValueStore");
        } catch (IOException e) {
            System.out.println("Failed to load RDB file into KeyValueStore: " + e.getMessage());
        }
    }

    public static void sendHandshakeToMaster(String host, int port) {
        try {
            System.out.println("Sending handshake to master");
            Socket socket = new Socket(host, port);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            sendCommand(out, "PING");
            validateResponse(in, "PONG");

            sendCommand(out, "REPLCONF", "listening-port", String.valueOf(Main.port));
            validateResponse(in, "OK");

            sendCommand(out, "REPLCONF", "capa", "psync2");
            validateResponse(in, "OK");

            sendCommand(out, "PSYNC", "?", "-1");

            List<String> response = CommandParser.process(in, new ArrayList<>());
            if (response.get(0).startsWith("FULLRESYNC")) {
                System.out.println("Received FULLRESYNC response from master");

                // Read the RDB file from the master. It will be of the form $<length>\r\n<bytes>
                in.readByte(); // Consume the '$' byte
                int rdbLength = CommandParser.readIntCRLF(in);
                byte[] rdbFileData = new byte[rdbLength];
                in.readFully(rdbFileData);
            
                // Write the RDB file to disk
                CommandExecutor.writeRdbFile(rdbFileData, "dump.rdb");
                
                System.out.println("RDB file loaded successfully");

                CommandParser.totalCommandBytesProcessed = 0;
            } else {
                throw new RuntimeException("Unexpected response from master: " + response.get(0));
            }

            masterInputStream = in;
            masterOutputStream = out;

            System.out.println("Handshake with master successful");

        } catch (IOException e) {
            System.out.println("Failed to send handshake to master: " + e.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void sendCommand(DataOutputStream out, String... command) throws IOException {
        CommandExecutor.writeArray(out, command);
    }

    private static void validateResponse(DataInputStream in, String expectedResponse) throws Exception {
        List<String> response = CommandParser.process(in, new ArrayList<>());
        if (!response.get(0).equals(expectedResponse)) {
            throw new RuntimeException("Failed to handshake with master: expected " + expectedResponse + ", received " + response.get(0) + " instead");
        }
    }

}
