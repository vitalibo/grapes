package com.github.vitalibo.grapes.processing.core.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class GraphMLOutputFormatTest {

    @Mock
    private TaskAttemptContext mockTaskAttemptContext;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private Path mockPath;
    @Mock
    private FileSystem mockFileSystem;
    @Mock
    private FSDataOutputStream mockFSDataOutputStream;

    private GraphMLOutputFormat spyGraphMLOutputFormat;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        spyGraphMLOutputFormat = Mockito.spy(new GraphMLOutputFormat());
    }

    @Test
    public void testGetRecordWriter() throws IOException {
        Mockito.when(mockTaskAttemptContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.doReturn(mockPath).when(spyGraphMLOutputFormat).getDefaultWorkFile(Mockito.any(), Mockito.anyString());
        Mockito.when(mockPath.getFileSystem(Mockito.any())).thenReturn(mockFileSystem);
        Mockito.when(mockFileSystem.create(Mockito.any(), Mockito.anyBoolean())).thenReturn(mockFSDataOutputStream);

        RecordWriter<IntWritable, ArrayPrimitiveWritable> actual =
            spyGraphMLOutputFormat.getRecordWriter(mockTaskAttemptContext);

        Assert.assertNotNull(actual);
        Assert.assertTrue(actual instanceof GraphMLRecordWriter);
        Mockito.verify(mockTaskAttemptContext).getConfiguration();
        Mockito.verify(spyGraphMLOutputFormat).getDefaultWorkFile(mockTaskAttemptContext, ".xml");
        Mockito.verify(mockPath).getFileSystem(mockConfiguration);
        Mockito.verify(mockFileSystem).create(mockPath, false);
    }

}
