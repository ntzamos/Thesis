package com.di.thesis.core.listener;

import com.di.thesis.core.entities.Tasks;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class StartupShutdownListener {

    public static Tasks tasks;
    public static String CONFIG_FILE = System.getProperty("user.home") + "/mytasks.csv";

    @PostConstruct
    public void startup() {
        System.out.println("=================================");
        System.out.println("Server Started!");
        System.out.println("Loading stored tasks...");
        System.out.println("=================================");

        tasks = new Tasks();
        tasks.loadTasks();
    }

    @PreDestroy
    public void shutdown() {
        System.out.println("=================================");
        System.out.println("Server is shutting down!");
        System.out.println("Cancelling active tasks and killing executor...");
        System.out.println("=================================");

        for(Map.Entry<Integer, ScheduledFuture<?>> s: tasks.getTasksMapping().entrySet())
            s.getValue().cancel(false);

        tasks.getTasksScheduler().shutdownNow();
    }
}
