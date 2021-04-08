package com.github.vitalibo.grapes.processing.infrastructure.vk;

import com.github.vitalibo.grapes.processing.core.io.UserInputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VkUserRecordReaderTest {

    @Mock
    private UserInputSplit mockUserInputSplit;
    @Mock
    private VkClient mockVkClient;
    @Mock
    private TaskAttemptContext mockTaskAttemptContext;
    @Mock
    private Configuration mockConfiguration;

    private Queue<Integer> backlog;
    private Queue<Map.Entry<Integer, List<Integer>>> queue;
    private RecordReader<IntWritable, ArrayPrimitiveWritable> reader;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        backlog = new LinkedList<>();
        queue = new LinkedList<>();
        reader = new VkUserRecordReader(
            new IntWritable(), new ArrayPrimitiveWritable(), backlog, queue,
            new AtomicReference<>(), (s, c) -> mockVkClient);
    }

    @Test
    public void testInitialize() throws IOException, InterruptedException {
        Mockito.when(mockTaskAttemptContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(mockUserInputSplit.getUsers()).thenReturn(new int[]{14, 23, 32, 41});

        reader.initialize(mockUserInputSplit, mockTaskAttemptContext);

        Assert.assertEquals(backlog, Arrays.asList(14, 23, 32, 41));
    }

    @Test
    public void testForEach() throws IOException, InterruptedException {
        queue.add(entry(1, Arrays.asList(11, 12, 13)));
        queue.add(entry(2, Arrays.asList(21, 22)));
        Mockito.when(mockUserInputSplit.getUsers()).thenReturn(new int[]{3, 4, 5});
        Mockito.doAnswer(answer -> {
            int id = (int) answer.getArgument(0, Supplier.class).get();
            return Collections.singletonMap(id, IntStream.range(0, id).boxed().collect(Collectors.toList()));
        }).when(mockVkClient).friends(Mockito.any(Supplier.class));
        reader.initialize(mockUserInputSplit, mockTaskAttemptContext);

        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(reader.getCurrentKey().get(), 1);
        Assert.assertEquals(reader.getCurrentValue().get(), new int[]{11, 12, 13});
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(reader.getCurrentKey().get(), 2);
        Assert.assertEquals(reader.getCurrentValue().get(), new int[]{21, 22});
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(reader.getCurrentKey().get(), 3);
        Assert.assertEquals(reader.getCurrentValue().get(), new int[]{0, 1, 2});
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(reader.getCurrentKey().get(), 4);
        Assert.assertEquals(reader.getCurrentValue().get(), new int[]{0, 1, 2, 3});
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(reader.getCurrentKey().get(), 5);
        Assert.assertEquals(reader.getCurrentValue().get(), new int[]{0, 1, 2, 3, 4});
        Assert.assertFalse(reader.nextKeyValue());
    }

    @DataProvider
    public Object[][] samples() {
        return new Object[][]{
            {10, 0, 0f}, {8, 2, 0f}, {8, 1, 0.1f}, {7, 0, 0.3f}, {5, 1, 0.4f},
            {0, 4, 0.6f}, {1, 1, 0.8f}, {0, 0, 1.0f}
        };
    }

    @Test(dataProvider = "samples")
    public void testProgress(int sizeBacklog, int sizeQueue, float expected) throws IOException, InterruptedException {
        Mockito.when(mockUserInputSplit.getLength()).thenReturn(10L);
        Mockito.when(mockUserInputSplit.getUsers()).thenReturn(new int[0]);
        IntStream.range(0, sizeBacklog).forEach(i -> backlog.add(i));
        IntStream.range(0, sizeQueue).forEach(i -> queue.add(entry(i, Collections.emptyList())));

        reader.initialize(mockUserInputSplit, mockTaskAttemptContext);
        float actual = reader.getProgress();

        Assert.assertEquals(actual, expected);
    }

    private static <K, V> Map.Entry<K, V> entry(K k, V v) {
        return new AbstractMap.SimpleEntry<>(k, v);
    }

}
