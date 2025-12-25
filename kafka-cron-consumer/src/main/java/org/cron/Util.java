// java
package org.cron;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Util {
    private static final String DEFAULT_FILE = "app.properties";

    private Util() {
    }

    public static Properties loadApplicationProperties() {
        Properties props = new Properties();
        try (InputStream in = Util.class.getClassLoader().getResourceAsStream(DEFAULT_FILE)) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException ignored) {
        }
        return props;
    }

    public static String getProperty(String key, String defaultValue) {
        return loadApplicationProperties().getProperty(key, defaultValue);
    }
}
