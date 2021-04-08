package com.github.vitalibo.grapes.processing.core.io;

import com.github.vitalibo.grapes.processing.infrastructure.vk.VkUserRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

public class SocialNetworkInputFormatTest {

    @Mock
    private JobContext mockJobContext;
    @Mock
    private TaskAttemptContext mockTaskAttemptContext;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private InputSplit mockInputSplit;

    private SocialNetworkInputFormat inputFormat;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        inputFormat = new SocialNetworkInputFormat();
    }

    @Test
    public void testGetSplits() {
        Mockito.when(mockJobContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(mockConfiguration.getInt("grapes.split.startId", 1)).thenReturn(4);
        Mockito.when(mockConfiguration.getInt("grapes.split.endId", 1000)).thenReturn(14);
        Mockito.when(mockConfiguration.getInt("grapes.split.numbs", 1)).thenReturn(3);

        List<InputSplit> actual = inputFormat.getSplits(mockJobContext);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual.size(), 3);
        Assert.assertEquals(((UserInputSplit) actual.get(0)).getUsers(), new int[]{4, 5, 6});
        Assert.assertEquals(((UserInputSplit) actual.get(1)).getUsers(), new int[]{7, 8, 9});
        Assert.assertEquals(((UserInputSplit) actual.get(2)).getUsers(), new int[]{10, 11, 12, 13});
    }

    @Test
    public void testCreateRecordReader() {
        Mockito.when(mockTaskAttemptContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(mockConfiguration.getClass(Mockito.eq("grapes.recordReader.class"), Mockito.any(), Mockito.any(Class.class)))
            .thenReturn(VkUserRecordReader.class);

        RecordReader<IntWritable, ArrayPrimitiveWritable> actual =
            inputFormat.createRecordReader(mockInputSplit, mockTaskAttemptContext);

        Assert.assertNotNull(actual);
        Assert.assertTrue(actual instanceof VkUserRecordReader);
    }

}