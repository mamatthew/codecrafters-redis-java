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

    public String[] toArray() {
        String[] array = new String[args.length + 1];
        array[0] = command.getName();
        System.arraycopy(args, 0, array, 1, args.length);
        return array;
    }

    public int getLengthInBytes() {
        int length = command.getName().length();
        for (String arg : args) {
            length += arg.length();
        }
        return length + 1;
    }
}
