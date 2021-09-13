package com.mware.core.config.options;

import com.mware.core.config.ConfigOption;
import com.mware.core.config.OptionHolder;

import static com.mware.core.config.OptionChecker.disallowEmpty;
import static com.mware.core.config.OptionChecker.positiveInt;

public class RabbitMqOptions extends OptionHolder {
    public static final String DEFAULT_PORT = "5672";
    public static final String RABBITMQ_ADDR_PREFIX = "rabbitmq.addr";

    public static final ConfigOption<String> RABBITMQ_USERNAME = new ConfigOption<>(
            "rabbitmq.username",
            "Username to connect to RabbitMQ server",
            disallowEmpty(),
            String.class,
            "admin"
    );

    public static final ConfigOption<String> RABBITMQ_PASSWORD = new ConfigOption<>(
            "rabbitmq.password",
            "Password to connect to RabbitMQ server",
            disallowEmpty(),
            String.class,
            "admin"
    );

    public static final ConfigOption<Integer> RABBITMQ_PREFETCH_COUNT = new ConfigOption<>(
            "rabbitmq.prefetch.count",
            "How many items should be fetched from the queue",
            positiveInt(),
            Integer.class,
            10
    );

    private RabbitMqOptions() {
        super();
    }

    private static volatile RabbitMqOptions instance;

    public static synchronized RabbitMqOptions instance() {
        if (instance == null) {
            instance = new RabbitMqOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }
}
