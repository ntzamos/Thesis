package com.di.thesis.core.entities;

import com.di.thesis.core.listener.StartupShutdownListener;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class Tasks {

    private Integer tasks_counter = 0;

    // Keeping the form fields here
    private String [] fields = {"id", "name", "server_address", "server_port", "delimeter", "unique_keys" ,
            "keep_deletes", "time", "has_header", "db_address", "db_name", "username",
            "password", "is_mysql", "first_time", "active"};

    // Tasks list
    private List<Task> tasks = new ArrayList<>();

    // Maps id to Task entity in order to be able to start and stop it
    private Map<Integer, ScheduledFuture<?>> tasksMapping = new HashMap<>();

    // Scheduler to execute the tasks every X seconds
    private ScheduledExecutorService tasksScheduler = new ScheduledThreadPoolExecutor(4);

    /**
     * addTask is used to add a Task entity to the SceduledExecutor object.
     * It also adds the Task entity to tasks List where all the Tasks entities are stored.
     * @param task: The Runnable task entity
     */
    public void addTask(Task task) {

        // Get Interval time
        Integer seconds = Integer.valueOf(task.get("time"));

        // Schedule task
        if(task.get("active") == null || task.get("active").equals("1")){
            ScheduledFuture<?> sf = tasksScheduler.scheduleAtFixedRate(task, seconds, seconds , TimeUnit.SECONDS);
            tasksMapping.put(tasks_counter, sf);
        }
        tasks.add(task);

        tasks_counter++;    // Increase num of tasks
    }

    /**
     * saveTask is used in order to save the Task entity to "mytasks.csv" file
     * where all the tasks are saved in order to be able to load them again if
     * the server shuts down.
     * @param task: The Runnable task entity
     */
    //
    public void saveTask(Task task) {

        String s = task.get(fields[0]);

        for(int i = 1; i < fields.length; i++) {
            s += ";" + task.get(fields[i]);
        }
        s += "\n";
        try {
            FileWriter pw = new FileWriter(StartupShutdownListener.CONFIG_FILE, true);
            pw.append(s);
            pw.close();
        }
        catch (IOException ex){
            throw new RuntimeException("Error at Task Save.");
        }
    }

    /**
     * loadTasks is user to load the Task entities stored at "mytasks.csv" file
     * at server startup. It also calls the addTask(Task) method in order to reschedule
     * the active tasks through the ScheduledExecutor object.
     */
    public void loadTasks() {

        BufferedReader fileReader;

        String line;

        //Create the file reader
        try {
            File f = new File(StartupShutdownListener.CONFIG_FILE);
            if(!f.exists())
                f.createNewFile();

            fileReader = new BufferedReader(new FileReader(StartupShutdownListener.CONFIG_FILE));

            while ((line = fileReader.readLine()) != null){

                Map<String, String> parameters = new HashMap<>();
                String[] tok = line.trim().split(";");

                for(int i = 0; i < tok.length; i++) {
                    parameters.put(fields[i], tok[i]);
                }

                Task task = new Task(parameters);
                addTask(task);
            }

            fileReader.close();
        }
        catch (IOException ex){
            throw new RuntimeException("Error at Reading File");
        }
    }

    public Integer getTasks_counter() {
        return tasks_counter;
    }

    public String[] getFields() {
        return fields;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    public Map<Integer, ScheduledFuture<?>> getTasksMapping() {
        return tasksMapping;
    }

    public ScheduledExecutorService getTasksScheduler() {
        return tasksScheduler;
    }
}
