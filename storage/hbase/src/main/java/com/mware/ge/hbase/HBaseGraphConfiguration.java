package com.mware.ge.hbase;

import com.mware.ge.GeException;
import com.mware.ge.store.StorableGraphConfiguration;
import com.mware.ge.util.GeLogger;
import com.mware.ge.util.GeLoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.HashMap;
import java.util.Map;

public class HBaseGraphConfiguration extends StorableGraphConfiguration {
    private static final GeLogger LOGGER = GeLoggerFactory.getLogger(HBaseGraphConfiguration.class);

    public HBaseGraphConfiguration(Map<String, Object> config) {
        super(config);
    }

    public HBaseGraphConfiguration(Configuration configuration, String prefix) {
        super(toMap(configuration, prefix));
    }

    private static Map<String, Object> toMap(Configuration configuration, String prefix) {
        Map<String, Object> map = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                key = key.substring(prefix.length());
            }
            map.put(key, entry.getValue());
        }
        return map;
    }

    public Connection createConnection() {
        try {
            Configuration config = HBaseConfiguration.create();
            config.clear();
            config.set("hbase.zookeeper.quorum", "localhost");
            config.set("hbase.zookeeper.property.clientPort", "2181");
            config.set("hbase.master", "localhost:60000");
            return ConnectionFactory.createConnection(config);
        }  catch (Exception ex) {
            throw new GeException(
                    String.format("Could not connect to HBase zookeeper servers [%s]", this.getZookeeperServers()),
                    ex
            );
        }
    }
}
