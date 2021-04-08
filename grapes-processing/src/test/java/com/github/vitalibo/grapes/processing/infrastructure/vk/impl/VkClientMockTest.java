package com.github.vitalibo.grapes.processing.infrastructure.vk.impl;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VkClientMockTest {

    @Mock
    private UserInputSplit mockUserInputSplit;
    @Mock
    private Configuration mockConfiguration;

    private VkClientMock vk;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
    }

    @Test
    public void testFriendsPseudorandom() {
        Mockito.when(mockUserInputSplit.getId()).thenReturn(5);
        Mockito.when(mockConfiguration.get("grapes.vk.mock.pseudorandomSeed.5")).thenReturn("123");
        vk = new VkClientMock(mockUserInputSplit, mockConfiguration);

        Map<Integer, List<Integer>> actual = vk.friends(Arrays.asList(7, 2, 33));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 3);
        Assert.assertEquals(actual.get(7).size(), 32);
        Assert.assertEquals(actual.get(7).subList(0, 4), Arrays.asList(8403, 1992, 9558, 7195));
        Assert.assertEquals(actual.get(2).size(), 33);
        Assert.assertEquals(actual.get(2).subList(0, 4), Arrays.asList(8245, 5965, 9668, 2239));
        Assert.assertEquals(actual.get(33).size(), 127);
        Assert.assertEquals(actual.get(33).subList(0, 4), Arrays.asList(9990, 2040, 7532, 938));
    }

    @Test(invocationCount = 10)
    public void testFriendsRandom() {
        Mockito.when(mockUserInputSplit.getId()).thenReturn(5);
        vk = new VkClientMock(mockUserInputSplit, mockConfiguration);

        Map<Integer, List<Integer>> actual = vk.friends(Collections.singletonList(259));

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 1);
        Assert.assertNotNull(actual.get(259));
        Assert.assertTrue(actual.get(259).size() < 150);
        for (Integer friend : actual.get(259)) {
            Assert.assertTrue(friend > 0);
            Assert.assertTrue(friend < 10000);
        }
    }

}
