package com.mware.core.model.plugin;

import com.mware.core.orm.Entity;
import com.mware.core.orm.Field;
import com.mware.core.orm.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity(tableName = "pluginstate")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PluginState {
    @Id
    private String clazz;

    @Field
    private Boolean enabled;

    @Field
    private Boolean systemPlugin;
}
