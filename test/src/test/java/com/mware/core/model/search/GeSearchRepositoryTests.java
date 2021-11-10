package com.mware.core.model.search;

import com.google.common.collect.Sets;
import com.mware.core.TestBaseWithInjector;
import com.mware.core.bootstrap.InjectHelper;
import com.mware.core.exception.BcAccessDeniedException;
import com.mware.core.exception.BcException;
import com.mware.core.model.clientapi.dto.ClientApiSearch;
import com.mware.core.model.clientapi.dto.ClientApiSearchListResponse;
import com.mware.core.model.clientapi.dto.Privilege;
import com.mware.core.model.user.PrivilegeRepository;
import com.mware.core.model.user.UserRepository;
import com.mware.core.model.workspace.Workspace;
import com.mware.core.model.workspace.WorkspaceRepository;
import com.mware.core.user.SystemUser;
import com.mware.core.user.User;
import com.mware.ge.util.GeAssert;
import net.minidev.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.mware.ge.util.IterableUtils.count;

public class GeSearchRepositoryTests extends TestBaseWithInjector {
    GeSearchRepository searchRepository;
    PrivilegeRepository privilegeRepository;
    User u1;
    Workspace w1;
    User u2;
    Workspace w2;

    public void before() {
        super.before();
        searchRepository = (GeSearchRepository) InjectHelper.getInstance(SearchRepository.class);
        privilegeRepository = InjectHelper.getInstance(PrivilegeRepository.class);

        u1 = InjectHelper.getInstance(UserRepository.class).addUser("u1", "u1", "", "");
        w1 = InjectHelper.getInstance(WorkspaceRepository.class).add(u1);
        u2 = InjectHelper.getInstance(UserRepository.class).addUser("u2", "u2", "", "");
        w2 = InjectHelper.getInstance(WorkspaceRepository.class).add(u2);
    }

    @Test
    public void testUserSearchAccess() {
        JSONObject searchParams = createSearchParams();

        // create a user search
        String id = searchRepository.saveSearch(null, "s1", "http://localhost", searchParams, u1);
        ClientApiSearch savedSearch = searchRepository.getSavedSearch(id, u1);
        Assert.assertEquals("s1", savedSearch.name);
        Assert.assertEquals("http://localhost", savedSearch.url);
        Assert.assertEquals(searchParams.toString(), new JSONObject(savedSearch.parameters).toString());

        // should not be global
        Assert.assertFalse(searchRepository.isSearchGlobal(id, getAuthorizations()));

        // must be avail for me in any workspace
        savedSearch = searchRepository.getSavedSearchOnWorkspace(id, u1, w1.getWorkspaceId());
        Assert.assertEquals("s1", savedSearch.name);
        savedSearch = searchRepository.getSavedSearchOnWorkspace(id, u1, w2.getWorkspaceId());
        Assert.assertEquals("s1", savedSearch.name);

        // not avail to u2 because he does not have access to w1
        savedSearch = searchRepository.getSavedSearchOnWorkspace(id, u2, w1.getWorkspaceId());
        Assert.assertNull(savedSearch);
        savedSearch = searchRepository.getSavedSearchOnWorkspace(id, u2, w2.getWorkspaceId());
        Assert.assertNull(savedSearch);

        ClientApiSearchListResponse savedSearches = searchRepository.getSavedSearches(u1);
        Assert.assertEquals(1, savedSearches.searches.size());

        savedSearches = searchRepository.getSavedSearches(u2);
        Assert.assertEquals(0, savedSearches.searches.size());
    }

    @Test
    public void testUserSearchUpdate() {
        JSONObject searchParams = createSearchParams();

        String id1 = searchRepository.saveSearch(null, "s1", "http://localhost", searchParams, u1);
        Assert.assertFalse(searchRepository.isSearchGlobal(id1, getAuthorizations()));
        try {
            searchRepository.saveSearch(null, "s1", "http://localhost", searchParams, u1);
            throw new IllegalStateException("Should have thrown an exception");
        } catch (BcException ex) {
            // expected
        }

        String id2 = searchRepository.saveSearch(null, "s2", "http://localhost", searchParams, u1);
        Assert.assertFalse(searchRepository.isSearchGlobal(id2, getAuthorizations()));

        try {
            searchRepository.saveSearch(id1, "s2", "http://localhost", searchParams, u1);
            throw new IllegalStateException("Should have thrown an exception");
        } catch (BcException ex) {
            // expected
        }

        ClientApiSearch s1 = searchRepository.getSavedSearch(id1, u1);
        searchRepository.saveSearch(id1, s1.name, "http://localhost2", new JSONObject(s1.parameters), u1);
        s1 = searchRepository.getSavedSearch(id1, u1);
        Assert.assertEquals("s1", s1.name);
        Assert.assertEquals("http://localhost2", s1.url);
        Assert.assertEquals(searchParams.toString(), new JSONObject(s1.parameters).toString());

        try {
            searchRepository.deleteSearch(id1, u2);
            throw new IllegalStateException("Should have thrown an exception");
        } catch (BcAccessDeniedException ex) {
            // expected
        }

        searchRepository.deleteSearch(id1, u1);
        s1 = searchRepository.getSavedSearch(id1, u1);
        Assert.assertNull(s1);
    }

    @Test
    public void testGlobalSearch() {
        JSONObject searchParams = createSearchParams();

        try {
            // u1 needs Privilege.SEARCH_SAVE_GLOBAL
            searchRepository.saveGlobalSearch(null, "gs1", "http://localhost", searchParams, u1);
            throw new IllegalStateException("Should have thrown: User does not have the privilege to save a global search");
        } catch (BcAccessDeniedException ex) {
            // expected
        }

        privilegeRepository.setPrivileges(u1, Sets.newHashSet(Privilege.SEARCH_SAVE_GLOBAL), new SystemUser());
        String id1 = searchRepository.saveSearch(null, "s1", "http://localhost", searchParams, u1);
        String gid1 = searchRepository.saveGlobalSearch(null, "s1", "http://localhost", searchParams, u1);

        ClientApiSearch s1 = searchRepository.getSavedSearch(id1, u1);
        try {
            // convert private search to global
            searchRepository.saveGlobalSearch(s1.id, s1.name, s1.url, new JSONObject(s1.parameters), u1);
            throw new IllegalStateException("Should have thrown: An existing global saved search with the same name already exist");
        }  catch (BcException ex) {
            // expected
        }

        // convert private search to global search
        String gid2 = searchRepository.saveGlobalSearch(s1.id, "s1g", s1.url, new JSONObject(s1.parameters), u1);
        Assert.assertEquals(0, count(searchRepository.getUserSavedSearches(u1, searchRepository.getAuhorizations(u1))));
        Assert.assertEquals(2, count(searchRepository.getGlobalSavedSearches(searchRepository.getAuhorizations(u1))));

        try {
            // try to save a global search as a private search
            searchRepository.saveSearch(gid2, "s1g", "http://localhost2", new JSONObject(s1.parameters), u1);
            throw new IllegalStateException("Should have thrown: A global search with the same ID already exists");
        }  catch (BcAccessDeniedException ex) {
            // expected
        }

        privilegeRepository.setPrivileges(u2, Sets.newHashSet(Privilege.SEARCH_SAVE_GLOBAL), new SystemUser());
        try {
            searchRepository.deleteSearch(gid2, u2);
            throw new IllegalStateException("Should have thrown: User does not own this global search");
        }  catch (BcAccessDeniedException ex) {
            // expected
        }

        searchRepository.deleteSearch(gid2, u1);
        Assert.assertEquals(1, count(searchRepository.getGlobalSavedSearches(searchRepository.getAuhorizations(u1))));
        Assert.assertEquals(1, count(searchRepository.getGlobalSavedSearches(searchRepository.getAuhorizations(u2))));
    }

    private static JSONObject createSearchParams() {
        JSONObject searchParams = new JSONObject();
        Map<String, Object> conceptTypes = new HashMap() {{
            put("iri", "document");
            put("includeChildNodes", false);
        }};
        Map<String, Object> filter = new HashMap() {{
            put("propertyId", "title");
            put("predicate", "~");
            put("values", Collections.singletonList("v1"));
            put("metadata", "");
        }};
        Map<String, Object> logicalSourceString = new HashMap() {{
            put("operator", null);
            put("id", 0);
        }};
        searchParams.put("conceptTypes", new JSONArray().appendElement(new JSONObject(conceptTypes)));
        searchParams.put("q", "*");
        searchParams.put("filter", new JSONArray().appendElement(new JSONObject(filter)));
        searchParams.put("refinement", new JSONArray());
        searchParams.put("logicalSourceString", new JSONArray().appendElement(new JSONObject(logicalSourceString)));

        return searchParams;
    }
}
