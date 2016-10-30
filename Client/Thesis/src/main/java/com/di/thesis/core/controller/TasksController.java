package com.di.thesis.core.controller;

import com.di.thesis.core.entities.Task;
import com.di.thesis.core.listener.StartupShutdownListener;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Controller
public class TasksController {

    @GetMapping(value = "/")
    String index() {
        return "index";
    }


    /**
     * Catches the POST request that contains the Task info provided by the user using
     * the interface. Creates the Task entity, initializes it and stores it to "mytasks.csv" file.
     * @param requestParams The form parameters in a map
     * @return return back to home page
     */
    @PostMapping(value = "createTask")
    String createTask(@RequestParam Map<String,String> requestParams) {

        Task task = new Task(requestParams);

        // Add extra default fields
        task.put("id", String.valueOf(StartupShutdownListener.tasks.getTasks_counter()));
        task.put("first_time", "1");
        task.put("active", "1");

        StartupShutdownListener.tasks.addTask(task);     // Add task to the Scheduler
        StartupShutdownListener.tasks.saveTask(task);    // Save task to file

        return "index";
    }

    /**
     * Pauses the Task that the user requested through the "Stop" button
     * of the interface.
     * @param id The id of the task to be pauses
     * @return return back to home page
     */
    @PostMapping(value = "pauseTask")
    String pauseTask(@RequestParam("id") String id) {

        List<Task> tasks = StartupShutdownListener.tasks.getTasks();
        String[] fields = StartupShutdownListener.tasks.getFields();

        try {
            synchronized (this) {
                FileWriter pw = new FileWriter(StartupShutdownListener.CONFIG_FILE, false);
                for (Task task : tasks) {

                    if (task.get("id").equals(id)) {
                        task.put("active", "0");
                    }

                    String s = task.get(fields[0]);
                    for (int i = 1; i < fields.length; i++)
                        s += ";" + task.get(fields[i]);

                    s += "\n";
                    pw.append(s);
                }
                pw.close();
            }
        } catch (IOException io) {
            throw new RuntimeException("Error at pauseTask");
        }

        StartupShutdownListener.tasks.getTasksMapping().get( Integer.valueOf( id ) ).cancel(false);

        return "index";
    }

    /**
     * Reschedules the stopped Task that the user requested through the "Start" button
     * of the interface.
     * @param id The id of the task to be resumed
     * @return return back to home page
     */
    @PostMapping(value = "resumeTask")
    String resumeTask(@RequestParam("id") String id) {

        List<Task> tasks = StartupShutdownListener.tasks.getTasks();

        String[] fields = StartupShutdownListener.tasks.getFields();

        Task thisTask = null;

        try {
            synchronized (this) {
                FileWriter pw = new FileWriter(StartupShutdownListener.CONFIG_FILE, false);
                for (Task task : tasks) {

                    if (task.get("id").equals(id)) {
                        task.put("active", "1");
                        thisTask = task;
                    }

                    String s = task.get(fields[0]);
                    for (int i = 1; i < fields.length; i++)
                        s += ";" + task.get(fields[i]);

                    s += "\n";
                    pw.append(s);
                }
                pw.close();
            }
        } catch (IOException io) {
            throw new RuntimeException("Error at resumeTask");
        }

        Integer seconds = Integer.valueOf( thisTask.get("time") );
        ScheduledFuture<?> sf = StartupShutdownListener.tasks.getTasksScheduler().scheduleAtFixedRate(thisTask, seconds, seconds , TimeUnit.SECONDS);
        StartupShutdownListener.tasks.getTasksMapping().put(Integer.valueOf( id ), sf );

        return "index";
    }
}
