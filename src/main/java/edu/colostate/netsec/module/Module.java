package edu.colostate.netsec.module;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import edu.colostate.netsec.BgpmonOuterClass;

public abstract class Module {
    protected String moduleId;
    private Map<String, Module> modules;
    private Timer timer;
    
    public Module(String moduleId, Map<String, Module> modules) {
        this.moduleId = moduleId;
        this.modules = modules;
    }

    public void schedule(int seconds) {
        this.timer = new Timer();
        this.timer.scheduleAtFixedRate(
            new TimerTask() {
                @Override
                public void run() {
                    execute();
                }
            },
            0,
            seconds * 1000
        );
    }

    public void selfDestruct() {
        if(this.timer != null) {
            this.timer.cancel();
        }

        destroy();
        modules.remove(moduleId);
    }

    public abstract void execute();
    public abstract BgpmonOuterClass.Module getProtobufModule();
    public abstract void destroy();
}
