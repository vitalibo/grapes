package com.github.vitalibo.grapes.processing.core.mapper;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

public class DunbarNumberMapperTest {

    @Mock
    private DunbarNumberMapper.Context mockContext;
    @Captor
    private ArgumentCaptor<Text> captorText;

    private DunbarNumberMapper mapper;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        mapper = new DunbarNumberMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        mapper.map(new IntWritable(12), new ArrayPrimitiveWritable(new int[]{1, 3, 5, 7}), mockContext);

        Mockito.verify(mockContext).write(captorText.capture(), Mockito.eq(new IntWritable(1)));
        Assert.assertEquals(captorText.getValue(), new Text("4"));
    }

}