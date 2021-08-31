package com.mware.core.model.plugin;

import com.mware.core.orm.Entity;
import com.mware.core.orm.Field;
import com.mware.core.orm.Id;
import lombok.*;

@Entity(tableName = "pluginstate")
@Data
public class PluginState {
    @Id
    private String clazz;

    @Field
    private Boolean enabled;
}
