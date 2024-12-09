import java.util.List;

public class Command {
private final CommandName command;
    private final String[] args;
    public Command(String command, String[] args) {
        this.command = CommandName.fromName(command);
        this.args = args;
    }

    public Command(List<String> args) {
        this.command = CommandName.fromName(args.get(0));
        this.args = args.subList(1, args.size()).toArray(new String[0]);
    }

    public CommandName getCommand() {
        return command;
    }
    public String[] getArgs() {
        return args;
    }


}
