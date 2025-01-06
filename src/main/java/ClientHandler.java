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
        try (DataInputStream in = new DataInputStream(clientSocket.getInputStream());
             DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream())) {
            while (true) {
                Command command = CommandParser.parse(in);
                if (Main.replicaOutputs.contains(out)) {
                    CommandExecutor.execute(command, null); // Process silently for replicas
                } else {
                    CommandExecutor.execute(command, out);
                }
                if (command.getCommand().equals(CommandName.PSYNC)) {
                    Main.replicaOutputs.add(out);
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to read/write from client socket: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
