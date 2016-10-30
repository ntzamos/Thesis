package com.di.thesis.core.entities;

import com.di.thesis.core.listener.StartupShutdownListener;
import com.di.thesis.core.services.ForwardDeltaService;
import com.di.thesis.core.services.impl.ForwardDeltaServiceImpl;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task implements Runnable
{

    // Task fields
    private Map<String, String> map = new HashMap<>();

    public Task(Map<String, String> map) {
        this.map = map;
    }

    public String get(String key) {

        if (!map.containsKey(key))
            return "";

        return map.get(key);
    }

    public void put(String key, String value){
        map.put(key, value);
    }

    @Override
    public void run()
    {
        System.out.println("Running task with id: " + this.get("id"));

        ForwardDeltaService forwardDeltaService = new ForwardDeltaServiceImpl(this);
        forwardDeltaService.forward();

        System.out.println("Didn't Ignore");

        if(get("first_time").equals("1")) {

            put("first_time","0");

            String[] fields = StartupShutdownListener.tasks.getFields();

            synchronized (this) {
                try {
                    FileWriter pw = new FileWriter(StartupShutdownListener.CONFIG_FILE, false);
                    List<Task> tasks = StartupShutdownListener.tasks.getTasks();
                    for (Task task : tasks) {
                        String s = task.get(fields[0]);
                        for (int i = 1; i < fields.length; i++)
                            s += ";" + task.get(fields[i]);
                        s += "\n";
                        pw.append(s);
                    }
                    pw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

}
