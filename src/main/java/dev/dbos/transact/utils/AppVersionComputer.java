package dev.dbos.transact.utils;

import java.io.InputStream;
import java.security.MessageDigest;
import java.util.*;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AppVersionComputer {

    static Logger logger = LoggerFactory.getLogger(AppVersionComputer.class);

    public static String computeAppVersion(Set<Class<?>> registeredClasses) {
        try {
            MessageDigest hasher = MessageDigest.getInstance("SHA-256");

            /*
             * registry.values().stream() .map(wrapper -> wrapper.target.getClass())
             * .collect(Collectors.toSet());
             */

            // Sort by class name for deterministic ordering
            List<Class<?>> sortedClasses = registeredClasses.stream()
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
    private static String getClassBytecodeHash(Class<?> clazz) {
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

    private static String bytesToHex(byte[] bytes) {
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

    private static String getFallbackVersion() {
        return "unknown-" + System.currentTimeMillis();
    }
}
