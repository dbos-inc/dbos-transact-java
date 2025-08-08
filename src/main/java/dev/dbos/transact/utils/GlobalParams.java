package dev.dbos.transact.utils;

import dev.dbos.transact.execution.DBOSExecutor;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalParams {

    static Logger logger = LoggerFactory.getLogger(GlobalParams.class);

    private String appVersion;
    private String executorId;

    private DBOSExecutor dbosExecutor;
    private static GlobalParams instance;

    private GlobalParams(DBOSExecutor de) {
        this.dbosExecutor = de;
        appVersion = System.getenv("DBOS__APPVERSION") == null
                ? generateAppVersion()
                : System.getenv("DBOS__APPVERSION");
        executorId = System.getenv("DBOS__VMID") == null
                ? "local"
                : System.getenv("DBOS__VMID");
    }

    public static synchronized GlobalParams getInstance(DBOSExecutor de) {
        if (instance != null) {
            return instance;
        }
        instance = new GlobalParams(de);
        return instance;
    }

    public static synchronized GlobalParams getInstance() {
        if (instance != null) {
            return instance;
        } else {
            throw new RuntimeException("Run dbos launch first.");
        }

    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getExecutorId() {
        return executorId;
    }

    private String generateAppVersion() {
        // return "DEFAULT_APP_VERSION";
        return computeAppVersionSimplified();
    }

    private String computeAppVersionSimplified() {
        String manualVersion = System.getenv("DBOS__APPVERSION");
        if (manualVersion != null && !manualVersion.trim().isEmpty()) {
            return manualVersion.trim();
        }

        try {
            MessageDigest hasher = MessageDigest.getInstance("SHA-256");

            // Get unique target classes
            Set<Class<?>> uniqueClasses = dbosExecutor.getRegisteredClasses();

            /*
             * registry.values().stream() .map(wrapper -> wrapper.target.getClass())
             * .collect(Collectors.toSet());
             */

            // Sort by class name for deterministic ordering
            List<Class<?>> sortedClasses = uniqueClasses.stream()
                    .sorted(Comparator.comparing(Class::getName))
                    .collect(Collectors.toList());

            // Hash each unique class
            for (Class<?> clazz : sortedClasses) {
                String classHash = getClassBytecodeHash(clazz);
                hasher.update((clazz.getName() + ":" + classHash).getBytes("UTF-8"));
            }

            // Add DBOS version
            // hasher.update(("dbos:" + getDbosVersion()).getBytes("UTF-8"));

            return bytesToHex(hasher.digest());

        } catch (Exception e) {
            logger.warn("Failed to compute simplified app version", e);
            return getFallbackVersion();
        }
    }

    /**
     * Gets a hash of the class bytecode.
     */
    private String getClassBytecodeHash(Class<?> clazz) {
        try {
            // Get the class file as a resource
            String className = clazz.getName().replace('.', '/') + ".class";

            try (InputStream is = clazz.getClassLoader().getResourceAsStream(className)) {
                if (is != null) {
                    MessageDigest hasher = MessageDigest.getInstance("SHA-256");
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        hasher.update(buffer, 0, bytesRead);
                    }
                    return bytesToHex(hasher.digest());
                }
            }

            // Fallback: use class hashCode and serialVersionUID if available
            long classHash = clazz.hashCode();
            try {
                java.lang.reflect.Field serialVersionUID = clazz.getDeclaredField("serialVersionUID");
                serialVersionUID.setAccessible(true);
                classHash ^= serialVersionUID.getLong(null);
            } catch (Exception ignored) {
                // serialVersionUID not available, that's ok
            }

            return Long.toHexString(classHash);

        } catch (Exception e) {
            logger.debug("Error getting class bytecode hash for {}", clazz.getName(), e);
            return Integer.toHexString(clazz.getName().hashCode());
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private String getFallbackVersion() {
        return "unknown-" + System.currentTimeMillis();
    }
}
