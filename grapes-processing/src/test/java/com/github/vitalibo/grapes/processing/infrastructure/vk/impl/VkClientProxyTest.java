package com.github.vitalibo.grapes.processing.infrastructure.vk.impl;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.vk.api.sdk.actions.Execute;
import com.vk.api.sdk.actions.Friends;
import com.vk.api.sdk.client.VkApiClient;
import com.vk.api.sdk.client.actors.UserActor;
import com.vk.api.sdk.exceptions.ApiException;
import com.vk.api.sdk.exceptions.ClientException;
import com.vk.api.sdk.queries.execute.ExecuteBatchQuery;
import com.vk.api.sdk.queries.friends.FriendsGetQuery;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VkClientProxyTest {

    @Mock
    private VkApiClient mockVkApiClient;
    @Mock
    private UserActor mockUserActor;
    @Mock
    private UserInputSplit mockUserInputSplit;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private Map<String, String> mockProperties;
    @Mock
    private Execute mockExecute;
    @Mock
    private ExecuteBatchQuery mockExecuteBatchQuery;
    @Mock
    private Friends mockFriends;
    @Mock
    private FriendsGetQuery mockFriendsGetQuery;
    @Mock
    private JsonElement mockJsonElement;

    private VkClientProxy vk;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        vk = new VkClientProxy(mockConfiguration, new Gson(), mockVkApiClient, mockUserActor);
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            {5, 3, 2}, {5, 2, 1}, {4, 4, 0}, {3, 1, 0}, {22, 7, 1}
        };
    }

    @Test(dataProvider = "samples")
    public void testConstructor(int splitId, int actors, int expected) {
        Mockito.when(mockUserInputSplit.getId()).thenReturn(splitId);
        Mockito.when(mockConfiguration.getInt("grapes.vk.proxy.retryAttemptsNetworkErrorCount", 3))
            .thenReturn(3);
        Mockito.when(mockConfiguration.getInt("grapes.vk.proxy.retryAttemptsInvalidStatusCount", 5))
            .thenReturn(4);
        Mockito.when(mockConfiguration.getInt(Mockito.startsWith("grapes.vk.proxy.actors."), Mockito.anyInt()))
            .thenReturn(123);
        Mockito.when(mockConfiguration.get(Mockito.startsWith("grapes.vk.proxy.actors."))).thenReturn("token");
        Mockito.when(mockConfiguration.getPropsWithPrefix("grapes.vk.proxy.actors."))
            .thenReturn(mockProperties);
        Mockito.when(mockProperties.size()).thenReturn(actors * 2);

        VkClientProxy actual = new VkClientProxy(mockUserInputSplit, mockConfiguration);

        UserActor actor = actual.getActor();
        Assert.assertEquals(actor.getId(), (Integer) 123);
        Assert.assertEquals(actor.getAccessToken(), "token");
        Mockito.verify(mockConfiguration).getInt(String.format("grapes.vk.proxy.actors.%s.id", expected), 0);
        Mockito.verify(mockConfiguration).get(String.format("grapes.vk.proxy.actors.%s.accessToken", expected));
    }

    @Test
    public void testFriends() throws ClientException, ApiException {
        Mockito.when(mockVkApiClient.execute()).thenReturn(mockExecute);
        Mockito.when(mockExecute.batch(Mockito.eq(mockUserActor), Mockito.anyList())).thenReturn(mockExecuteBatchQuery);
        Mockito.when(mockVkApiClient.friends()).thenReturn(mockFriends);
        Mockito.when(mockFriends.get(mockUserActor)).thenReturn(mockFriendsGetQuery);
        Mockito.when(mockFriendsGetQuery.userId(Mockito.anyInt())).thenReturn(mockFriendsGetQuery);
        Mockito.when(mockExecuteBatchQuery.execute()).thenReturn(mockJsonElement);
        final JsonArray array = new JsonArray(3);
        array.add(response(2, 4, 6, 8));
        array.add(error());
        array.add(response(10, 5));
        Mockito.when(mockJsonElement.getAsJsonArray()).thenReturn(array);

        Map<Integer, List<Integer>> actual = vk.friends(Arrays.asList(12, 4, 8));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 3);
        Assert.assertEquals(actual.get(12), Arrays.asList(2, 4, 6, 8));
        Assert.assertEquals(actual.get(4), Collections.emptyList());
        Assert.assertEquals(actual.get(8), Arrays.asList(10, 5));
    }

    private static JsonElement response(Integer... items) {
        final JsonObject json = new JsonObject();
        json.addProperty("count", items.length);
        final JsonArray array = new JsonArray();
        for (Integer item : items) {
            array.add(item);
        }

        json.add("items", array);
        return json;
    }

    private static JsonElement error() {
        final JsonObject json = new JsonObject();
        json.addProperty("error", "some error");
        return json;
    }

}
