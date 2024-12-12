import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int port = 6379;
    private static final int THREAD_POOL_SIZE = 10;
    public static String rdbFilePath = null;

    public static void main(String[] args){
          setup(args);
          ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

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
        if (args.length != 4) {
            return;
        }
        if (args[0].equals("--dir") && args[2].equals("--dbfilename")) {
            rdbFilePath = args[1] + "/" + args[3];
        }
    }

}
