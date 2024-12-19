import com.beust.jcommander.Parameter;
import java.util.List;
import java.util.ArrayList;

public class CommandLineArgs {

    private static final int DEFAULT_PORT = 6379;

    @Parameter
    private List<String> parameters = new ArrayList<>();

    @Parameter(names = {"--port", "-p"}, description = "Port number to listen on")
    public int port = DEFAULT_PORT;

    @Parameter(names = {"--dir", "-d"}, description = "Directory to store RDB file")
    public String dir = null;

    @Parameter(names = {"--dbfilename", "-f"}, description = "Name of the RDB file")
    public String dbfilename = null;

    @Parameter(names = {"--replicaof", "-r"}, description = "Host and port of the master")
    public String replicaof = null;
}
