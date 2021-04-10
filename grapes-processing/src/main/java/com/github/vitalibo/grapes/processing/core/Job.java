package com.github.vitalibo.grapes.processing.core;

import com.github.vitalibo.grapes.processing.core.util.Throwing;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job.TaskStatusFilter;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

@Slf4j
public class Job {

    private final JobControl jobControl;
    private final List<ControlledJob> jobs;

    private Thread jobControlThread;

    public Job(JobControl jobControl) {
        Function<JobControl, LinkedList<ControlledJob>> jobsInProgress =
            reflectionGetPrivateField(JobControl.class, "jobsInProgress");
        this.jobControl = jobControl;
        this.jobs = jobsInProgress.apply(jobControl);
    }

    public void submit() {
        jobControlThread = new Thread(jobControl);
        jobControlThread.start();
    }

    public void waitForCompletion(boolean verbose) {
        if (jobControlThread == null) {
            submit();
        }

        if (verbose) {
            Function<org.apache.hadoop.mapreduce.Job, ?> jobState =
                reflectionGetPrivateField(org.apache.hadoop.mapreduce.Job.class, "state");

            for (ControlledJob controlledJob : jobs) {
                new Thread(() -> {
                    while (jobState.apply(controlledJob.getJob()) == org.apache.hadoop.mapreduce.Job.JobState.DEFINE) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ignored) {
                        }
                    }

                    monitorAndPrintJob(controlledJob.getJob());
                }).start();
            }
        }

        while (!jobControl.allFinished()) {
            try {
                Thread.sleep(1000);
            } catch (Exception ignored) {
            }
        }
    }

    @SneakyThrows
    void monitorAndPrintJob(org.apache.hadoop.mapreduce.Job job) {
        final JobID jobId = job.getJobID();
        final Configuration clientConf = job.getConfiguration();
        final TaskStatusFilter filter = org.apache.hadoop.mapreduce.Job.getTaskOutputFilter(clientConf);
        final int progMonitorPollIntervalMillis = org.apache.hadoop.mapreduce.Job.getProgressPollInterval(clientConf);
        logger.info("Running job: " + jobId);

        String lastReport = null;
        int eventCounter = 0;
        boolean reportedAfterCompletion = false;
        boolean reportedUberMode = false;

        while (!job.isComplete() || !reportedAfterCompletion) {
            if (job.isComplete()) {
                reportedAfterCompletion = true;
            } else {
                Thread.sleep(progMonitorPollIntervalMillis);
            }

            if (job.getStatus().getState() == JobStatus.State.PREP) {
                continue;
            }

            if (!reportedUberMode) {
                reportedUberMode = true;
                logger.info("Job {} running in uber mode : {}", jobId, job.isUber());
            }

            String report = String.format("Job %s   map %s reduce %s", jobId,
                StringUtils.formatPercent(job.mapProgress(), 0),
                StringUtils.formatPercent(job.reduceProgress(), 0));
            if (!report.equals(lastReport)) {
                logger.info(report);
                lastReport = report;
            }

            TaskCompletionEvent[] events = job.getTaskCompletionEvents(eventCounter, 10);
            eventCounter += events.length;
            printTaskEvents(job, events, filter);
        }

        if (job.isSuccessful()) {
            logger.info("Job {} completed successfully", jobId);
        } else {
            logger.info("Job {} failed with state {} due to: {}",
                jobId, job.getStatus().getState(), job.getStatus().getFailureInfo());
        }

        Counters counters = job.getCounters();
        if (counters != null) {
            logger.info("Job {} {}", jobId, counters);
        }
    }

    private static void printTaskEvents(org.apache.hadoop.mapreduce.Job job, TaskCompletionEvent[] events,
                                        TaskStatusFilter filter) throws IOException, InterruptedException {
        for (TaskCompletionEvent event : events) {
            switch (filter) {
                case NONE:
                    break;
                case SUCCEEDED:
                    if (event.getStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
                        logger.info(event.toString());
                    }
                    break;
                case FAILED:
                    if (event.getStatus() == TaskCompletionEvent.Status.FAILED) {
                        logger.info(event.toString());
                        TaskAttemptID taskId = event.getTaskAttemptId();
                        String[] taskDiagnostics = job.getTaskDiagnostics(taskId);
                        if (taskDiagnostics != null) {
                            for (String diagnostics : taskDiagnostics) {
                                System.err.println(diagnostics);
                            }
                        }
                    }
                    break;
                case KILLED:
                    if (event.getStatus() == TaskCompletionEvent.Status.KILLED) {
                        logger.info(event.toString());
                    }
                    break;
                case ALL:
                    logger.info(event.toString());
                    break;
            }
        }
    }

    @SneakyThrows
    private static <T, M> Function<T, M> reflectionGetPrivateField(Class<T> cls, String name) {
        Field field = cls.getDeclaredField(name);
        field.setAccessible(true);
        return (Throwing.Function<T, M>) o -> (M) field.get(o);
    }

}
