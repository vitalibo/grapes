package com.github.vitalibo.grapes.processing.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PathsTest {

    @Mock
    private Configuration mockConfiguration;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
    }

    @Test
    public void testCreateTempDirectory() {
        Mockito.when(mockConfiguration.get("hadoop.tmp.dir")).thenReturn("/foo");

        Path actual = Paths.createTempDirectory(mockConfiguration);

        Assert.assertNotNull(actual);
        Assert.assertTrue(("" + actual).matches("/foo/[a-z0-9]{12}"));
    }

}
