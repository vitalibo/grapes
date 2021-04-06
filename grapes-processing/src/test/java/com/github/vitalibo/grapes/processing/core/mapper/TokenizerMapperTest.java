package com.github.vitalibo.grapes.processing.core.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;

public class TokenizerMapperTest {

    @Mock
    private Text mockTextIn;
    @Mock
    private Text mockTextOut;
    @Mock
    private TokenizerMapper.Context mockContext;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private Counter mockCounter;
    @Captor
    private ArgumentCaptor<IntWritable> captorIntWritable;

    private TokenizerMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new TokenizerMapper(mockTextOut, new HashSet<>());
        Mockito.when(mockContext.getConfiguration()).thenReturn(mockConfiguration);
        Mockito.when(mockContext.getCounter(Mockito.anyString(), Mockito.anyString())).thenReturn(mockCounter);
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        Mockito.when(mockConfiguration.getBoolean("wordcount.case.sensitive", true))
            .thenReturn(false);
        Mockito.when(mockConfiguration.getBoolean("wordcount.skip.patterns", false))
            .thenReturn(false);
        Mockito.when(mockTextIn.toString()).thenReturn("foo bar baz Foo");

        mapper.setup(mockContext);
        mapper.map(new Object(), mockTextIn, mockContext);

        Mockito.verify(mockTextOut, Mockito.times(2)).set("foo");
        Mockito.verify(mockTextOut).set("bar");
        Mockito.verify(mockTextOut).set("baz");
        Mockito.verify(mockContext, Mockito.times(4))
            .write(Mockito.eq(mockTextOut), captorIntWritable.capture());
        Assert.assertEquals(captorIntWritable.getValue().get(), 1);
    }

}
