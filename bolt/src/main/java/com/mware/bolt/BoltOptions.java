package com.mware.bolt;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;

import java.time.Duration;

import static com.mware.core.config.OptionChecker.*;

public class BoltOptions extends OptionHolder {
    public static final ConfigOption<String> BOLT_HOST = new ConfigOption<>(
            "bolt.host",
            "Listen address for Bolt server",
            disallowEmpty(),
            "localhost"
    );

    public static final ConfigOption<Integer> BOLT_PORT = new ConfigOption<>(
            "bolt.port",
            "Listen port for Bolt server",
            rangeInt(1000, Integer.MAX_VALUE),
            10242
    );

    public static final ConfigOption<Integer> THREAD_POOL_MIN_SIZE = new ConfigOption<>(
            "bolt.threadPoolMinSize",
            "Minimum number of listen threads to keep in the thread pool, even if they are idle",
            positiveInt(),
            5
    );

    public static final ConfigOption<Integer> THREAD_POOL_MAX_SIZE = new ConfigOption<>(
            "bolt.threadPoolMaxSize",
            "Maximum number of listen threads to keep in the thread pool",
            positiveInt(),
            400
    );

    public static final ConfigOption<Duration> THREAD_POOL_KEEPALIVE = new ConfigOption<>(
            "bolt.threadPoolKeepalive",
            "The maximum time an idle thread in the thread pool will wait for new tasks",
            disallowEmpty(),
            Duration.ofMinutes(5)
    );

    public static final ConfigOption<String> ENCRYPTION_LEVEL = new ConfigOption<>(
            "bolt.encryptionLevel",
            "Encryption level to use",
            allowValues(
                    BoltConnector.EncryptionLevel.OPTIONAL.name(),
                    BoltConnector.EncryptionLevel.REQUIRED.name(),
                    BoltConnector.EncryptionLevel.DISABLED.name()
            ),
            BoltConnector.EncryptionLevel.OPTIONAL.name()
    );

    public static final ConfigOption<String> SSL_CERTIFICATE_FILE = new ConfigOption<>(
            "bolt.ssl.tls_certificate_file",
            "Path to the X.509 public certificate file",
            "bolt.cert"
    );

    public static final ConfigOption<String> SSL_KEY_FILE = new ConfigOption<>(
            "bolt.ssl.tls_key_file",
            "Path to the X.509 private key file",
            "bolt.key"
    );

    private BoltOptions() {
        super();
    }

    private static volatile BoltOptions instance;

    public static synchronized BoltOptions instance() {
        if (instance == null) {
            instance = new BoltOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
