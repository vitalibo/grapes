package com.github.vitalibo.grapes.processing.infrastructure.vk;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class VkClientTest {

    @Mock
    private Configuration mockConfiguration;
    @Mock
    private Supplier<Integer> mockSupplier;
    @Mock
    private UserInputSplit mockUserInputSplit;

    private VkClient spyVkClient;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        Mockito.when(mockConfiguration.getInt(Mockito.anyString(), Mockito.anyInt())).thenReturn(5);
        spyVkClient = Mockito.spy(new TestVkClient(mockUserInputSplit, mockConfiguration));
    }

    @Test
    public void testFriends() {
        Map<Integer, List<Integer>> expected = new HashMap<>();
        Mockito.doReturn(expected).when(spyVkClient).friends(Mockito.anyList());
        Mockito.when(mockSupplier.get()).thenReturn(1, 2, 3, null);

        Map<Integer, List<Integer>> actual = spyVkClient.friends(mockSupplier);

        Assert.assertSame(actual, expected);
        Mockito.verify(mockConfiguration).getInt("grapes.vk.batchSize", 25);
        Mockito.verify(mockSupplier, Mockito.times(5)).get();
        Mockito.verify(spyVkClient).friends(Arrays.asList(1, 2, 3));
    }

    @Test
    public void testNewInstance() {
        Mockito.when(mockConfiguration.getClass(Mockito.anyString(), Mockito.any(Class.class), Mockito.any()))
            .thenReturn(TestVkClient.class);

        VkClient actual = VkClient.newInstance(mockUserInputSplit, mockConfiguration);

        Assert.assertNotNull(actual);
        Assert.assertTrue(actual instanceof TestVkClient);
    }

    public static class TestVkClient extends VkClient {

        public TestVkClient(UserInputSplit split, Configuration configuration) {
            super(configuration);
        }

        public Map<Integer, List<Integer>> friends(List<Integer> ids) {
            throw new IllegalStateException();
        }

    }

}
