package com.mware.ge.nebula;

import com.vesoft.nebula.meta.IdName;
import com.vesoft.nebula.meta.Schema;
import com.vesoft.nebula.meta.TagItem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TestExtendedMetaClient {
    private ExtendedMetaClient metaClient;

    @Before
    public void before() {
        metaClient = new ExtendedMetaClient("127.0.0.1", 9559);
        metaClient.connect();
    }

    @Test
    public void testSpaces() throws Exception {
        String spaceName = "space-" + System.currentTimeMillis();
        metaClient.createSpace(spaceName, 4, 1);
        List<IdName> spaces = metaClient.getSpaces();
        Assert.assertTrue(
                spaces.stream().anyMatch(idn -> Arrays.equals(spaceName.getBytes(), idn.getName()))
        );
        metaClient.dropSpace(spaceName);
        spaces = metaClient.getSpaces();
        Assert.assertFalse(
                spaces.stream().anyMatch(idn -> Arrays.equals(spaceName.getBytes(), idn.getName()))
        );
    }

    @Test
    public void testTags() throws Exception {
        String spaceName = "space-" + System.currentTimeMillis();
        int spaceId = metaClient.createSpace(spaceName, 4, 1);

        Schema schema = new Schema();

        metaClient.dropSpace(spaceName);
    }
}
