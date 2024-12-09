import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    private static final int port = 6379;
    private static final int THREAD_POOL_SIZE = 10;

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
        // args be a sequence of key value pairs of the form --key value. add these to the key value store
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Invalid number of arguments");
        }
        KeyValueStore keyValueStore = KeyValueStore.getInstance();
        for (int i = 0; i < args.length; i += 2) {
            keyValueStore.put(args[i].substring(2), args[i + 1]);
        }
    }

}
