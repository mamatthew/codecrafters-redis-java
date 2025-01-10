import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            DataInputStream in = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());
            while (true) {
                Command command = CommandParser.parse(in);
                System.out.println("Received command from client: " + command.getCommand());
                if (Main.masterHostAndPort != null && Main.masterInputStream == null) {
                    String host = Main.masterHostAndPort.split(" ")[0];
                    int port = Integer.parseInt(Main.masterHostAndPort.split(" ")[1]);
                    Main.sendHandshakeToMaster(host, port);
                }
                if (command.getCommand().equals(CommandName.REPLCONF) && command.getArgs()[0].equalsIgnoreCase("ACK") && CommandExecutor.latch != null) {
                    // decrement the number of replicas that need to acknowledge
                    CommandExecutor.latch.countDown();
                    System.out.println("Number of replicas that need to acknowledge: " + CommandExecutor.latch.getCount());
                } else {
                    CommandExecutor.execute(command, out);
                }
                if (command.getCommand().equals(CommandName.PSYNC)) {
                    Main.replicaMap.put(out, in);
                }
                
            }
        } catch (Exception e) {
            System.out.println(Thread.currentThread().getName() + " Failed to read/write from client socket: " + e.getMessage());
        } finally {
            try {
                System.out.println("Closing client socket");
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
