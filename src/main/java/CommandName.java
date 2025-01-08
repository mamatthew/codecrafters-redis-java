public enum CommandName {
    PING("PING"),
    ECHO("ECHO"),
    GET("GET"),
    SET("SET"),
    CONFIG("CONFIG"),
    KEYS("KEYS"),
    INFO("INFO"),
    REPLCONF("REPLCONF"),
    PSYNC("PSYNC"),
    WAIT("WAIT");

    private String name;

    CommandName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static CommandName fromName(String name) {
        for (CommandName commandName : CommandName.values()) {
            if (commandName.getName().equalsIgnoreCase(name)) {
                return commandName;
            }
        }
        return null;
    }
}
