import com.beust.jcommander.JCommander;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int THREAD_POOL_SIZE = 10;
    public static String rdbFilePath = null;
    private static int port;
    static String masterHostAndPort;

    public static void main(String[] args){
          setup(args);
          ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
          System.out.println("Starting server on port " + port);
          try (ServerSocket serverSocket = new ServerSocket(port)) {
              serverSocket.setReuseAddress(true);
              while (true) {
                  // Wait for connection from client.
                  Socket clientSocket = serverSocket.accept();
                  threadPool.execute(new ClientHandler(clientSocket));
              }

            } catch (IOException e) {
              System.out.println("IOException: " + e.getMessage());
            } finally {
              threadPool.shutdown();
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
        }
        port = commandLineArgs.port;
        masterHostAndPort = commandLineArgs.replicaof;
        if (masterHostAndPort != null) {
            String host = masterHostAndPort.split(" ")[0];
            int port = Integer.parseInt(masterHostAndPort.split(" ")[1]);
            sendHandshakeToMaster(host, port);
        }
    }

    private static void sendHandshakeToMaster(String host, int port) {
        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
            CommandExecutor.writeArray(out, new String[]{"PING"});
        } catch (IOException e) {
            System.out.println("Failed to send handshake to master: " + e.getMessage());
        }
    }

}
