package com.github.vitalibo.grapes.processing;

import com.github.vitalibo.grapes.processing.core.Job;
import com.github.vitalibo.grapes.processing.infrastructure.Factory;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DriverTest {

    @Mock
    private Factory mockFactory;
    @Mock
    private Job mockJob;

    private Driver driver;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        driver = new Driver(mockFactory);
        Mockito.when(mockFactory.createWordCountJob(Mockito.any())).thenReturn(mockJob);
        Mockito.when(mockFactory.createSocialNetworkImportJob(Mockito.any())).thenReturn(mockJob);
        Mockito.when(mockFactory.createDijkstraAlgorithmJob(Mockito.any())).thenReturn(mockJob);
        Mockito.when(mockFactory.createGraphCapacityJob(Mockito.any())).thenReturn(mockJob);
    }

    @Test
    public void testRunWorldCountJob() throws Exception {
        driver.run(new String[]{"wordcount"});

        Mockito.verify(mockFactory).createWordCountJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createSocialNetworkImportJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createDijkstraAlgorithmJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createGraphCapacityJob(Mockito.any());
        Mockito.verify(mockJob).submit();
        Mockito.verify(mockJob).waitForCompletion(true);
    }

    @Test
    public void testRunSocialNetworkImportJob() throws Exception {
        driver.run(new String[]{"socnetimp"});

        Mockito.verify(mockFactory, Mockito.never()).createWordCountJob(Mockito.any());
        Mockito.verify(mockFactory).createSocialNetworkImportJob(Mockito.any());
        Mockito.verify(mockJob).submit();
        Mockito.verify(mockJob).waitForCompletion(true);
    }

    @Test
    public void testRunDijkstraAlgorithmJob() throws Exception {
        driver.run(new String[]{"dijkstra"});

        Mockito.verify(mockFactory, Mockito.never()).createWordCountJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createSocialNetworkImportJob(Mockito.any());
        Mockito.verify(mockFactory).createDijkstraAlgorithmJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createGraphCapacityJob(Mockito.any());
        Mockito.verify(mockJob).submit();
        Mockito.verify(mockJob).waitForCompletion(true);
    }

    @Test
    public void testRunGraphCapacityJob() throws Exception {
        driver.run(new String[]{"capacity"});

        Mockito.verify(mockFactory, Mockito.never()).createWordCountJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createSocialNetworkImportJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createDijkstraAlgorithmJob(Mockito.any());
        Mockito.verify(mockFactory).createGraphCapacityJob(Mockito.any());
        Mockito.verify(mockJob).submit();
        Mockito.verify(mockJob).waitForCompletion(true);
    }

    @Test
    public void testUnknownJob() throws Exception {
        IllegalArgumentException actual = Assert.expectThrows(IllegalArgumentException.class,
            () -> driver.run(new String[]{"foo"}));

        Assert.assertEquals(actual.getMessage(), "Unknown job name");
        Mockito.verify(mockFactory, Mockito.never()).createWordCountJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createSocialNetworkImportJob(Mockito.any());
        Mockito.verify(mockJob, Mockito.never()).submit();
        Mockito.verify(mockJob, Mockito.never()).waitForCompletion(true);
    }

    @Test
    public void testSubmitJobFailed() throws Exception {
        Mockito.doThrow(new RuntimeException("foo")).when(mockJob).submit();

        RuntimeException actual = Assert.expectThrows(RuntimeException.class,
            () -> driver.run(new String[]{"wordcount"}));

        Assert.assertEquals(actual.getMessage(), "foo");
        Mockito.verify(mockFactory).createWordCountJob(Mockito.any());
        Mockito.verify(mockFactory, Mockito.never()).createSocialNetworkImportJob(Mockito.any());
        Mockito.verify(mockJob).submit();
        Mockito.verify(mockJob, Mockito.never()).waitForCompletion(true);
    }

}
