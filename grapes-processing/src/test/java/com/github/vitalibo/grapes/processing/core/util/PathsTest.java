package com.github.vitalibo.grapes.processing.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.mockito.*;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PathsTest {

    @Mock
    public RemoteIterator<LocatedFileStatus> mockRemoteIterator;
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private FileSystem mockFileSystem;
    @Mock
    private FSDataOutputStream mockFSDataOutputStream;
    @Captor
    private ArgumentCaptor<Path> captorPath;
    @Mock
    private LocatedFileStatus mockLocatedFileStatus;

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

    @Test
    public void testCreateTextFile() throws IOException {
        Mockito.when(mockFileSystem.create(Mockito.any(Path.class))).thenReturn(mockFSDataOutputStream);

        Paths.createTextFile(mockFileSystem, "foo", "bar");

        Mockito.verify(mockFileSystem).create(captorPath.capture());
        Assert.assertEquals(captorPath.getValue(), new Path("foo"));
        Mockito.verify(mockFSDataOutputStream).write("bar".getBytes());
        Mockito.verify(mockFSDataOutputStream).close();
    }

    @Test
    public void testListFiles() throws IOException {
        Mockito.when(mockFileSystem.listFiles(Mockito.any(), Mockito.anyBoolean())).thenReturn(mockRemoteIterator);
        Mockito.when(mockRemoteIterator.hasNext()).thenReturn(true, true, false);
        Mockito.when(mockRemoteIterator.next()).thenReturn(mockLocatedFileStatus);
        Mockito.when(mockLocatedFileStatus.getPath()).thenReturn(new Path("bar"), new Path("baz"));

        List<Path> actual = Paths.listFiles(mockFileSystem, "foo", true);

        Assert.assertNotNull(actual);
        Assert.assertEquals(actual, Arrays.asList(new Path("bar"), new Path("baz")));
        Mockito.verify(mockFileSystem).listFiles(new Path("foo"), true);
    }

}
