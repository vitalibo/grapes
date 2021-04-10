package com.github.vitalibo.grapes.processing.core;

import lombok.SneakyThrows;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

public class JobTest {

    @Mock
    private ControlledJob mockControlledJob;
    @Mock
    private org.apache.hadoop.mapreduce.Job mockJob;

    private JobControl spyJobControl;
    private Job spyJob;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this).close();
        spyJobControl = Mockito.spy(new JobControl("foo"));
        spyJobControl.addJob(mockControlledJob);
        spyJob = Mockito.spy(new Job(spyJobControl));
    }

    @Test
    public void testSubmit() throws InterruptedException {
        Mockito.doNothing().when(spyJobControl).run();
        spyJob.submit();

        Thread.sleep(1000);
        Mockito.verify(spyJobControl).run();
    }

    @Test
    public void testWaitForCompletion() throws IllegalAccessException, InterruptedException {
        Mockito.doNothing().when(spyJob).submit();
        Mockito.doNothing().when(spyJob).monitorAndPrintJob(Mockito.any());
        Mockito.when(mockControlledJob.getJob()).thenReturn(mockJob);
        Field state = reflectionGetPrivateField(org.apache.hadoop.mapreduce.Job.class, "state");
        state.set(mockJob, org.apache.hadoop.mapreduce.Job.JobState.RUNNING);
        AtomicInteger atomic = new AtomicInteger(0);
        Mockito.doReturn(false, true).when(spyJobControl).allFinished();

        spyJob.waitForCompletion(true);

        Thread.sleep(5000);
        Mockito.verify(spyJob).submit();
        Mockito.verify(mockControlledJob, Mockito.times(2)).getJob();
        Mockito.verify(spyJob).monitorAndPrintJob(mockJob);
        Mockito.verify(spyJobControl, Mockito.times(2)).allFinished();
    }

    @SneakyThrows
    private static <T> Field reflectionGetPrivateField(Class<T> cls, String name) {
        Field field = cls.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

}
