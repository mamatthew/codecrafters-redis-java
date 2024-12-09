import java.util.Map;
import java.util.concurrent.*;

public class KeyValueStore {
    private final Map<String, Object> store;
    private ScheduledExecutorService executorService;
    private Map<String, ScheduledFuture> expirationTasks;
    private static KeyValueStore instance;

    private KeyValueStore() {
        store = new ConcurrentHashMap<>();
        executorService = Executors.newScheduledThreadPool(1);
        expirationTasks = new ConcurrentHashMap<>();
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

    public void put(String key, Object value, long ttlMilliseconds) {
        cancelExpiration(key);
        put(key, value);
        scheduleExpiration(key, ttlMilliseconds);
    }

    private void scheduleExpiration(String key, long ttlMilliseconds) {
        ScheduledFuture future = executorService.schedule(() -> {
            store.remove(key);
            expirationTasks.remove(key);
        }, ttlMilliseconds, TimeUnit.MILLISECONDS);
        expirationTasks.put(key, future);
    }

    public Object get(String key) {
        return store.get(key);
    }

    public void delete(String key) {
        cancelExpiration(key);
        store.remove(key);
    }

    private void cancelExpiration(String key) {
        ScheduledFuture future = expirationTasks.get(key);
        if (future != null) {
            future.cancel(false);
        }
    }

}
