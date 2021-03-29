package com.mware.core.model.longRunningProcess;

import com.mware.core.util.ClientApiConverter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.json.JSONObject;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeleteRestoreElementsQueueItem extends LongRunningProcessQueueItemBase {
    public static final String SEARCH_DELETE_ELEMENTS_TYPE = "delete-elements";
    public static final String SEARCH_RESTORE_ELEMENTS_TYPE = "restore-elements";

    private String savedSearchId;
    private boolean backup;
    private String savedSearchName;
    private String userId;
    private String[] authorizations;
    private String type;

    @Override
    public String getType() {
        return type;
    }

    public JSONObject toJson() {
        return new JSONObject(ClientApiConverter.clientApiToString(this));
    }
}

