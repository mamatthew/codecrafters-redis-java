import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyValueStore {
    private final Map<String, Object> store;

    private static KeyValueStore instance;

    private KeyValueStore() {
        store = new ConcurrentHashMap<>();
    }

    public static synchronized KeyValueStore getInstance() {
        if (instance == null) {
            instance = new KeyValueStore();
        }
        return instance;
    }

    public void put(String key, Object value) {
        store.put(key, value);
    }

    public Object get(String key) {
        return store.get(key);
    }

}
