package edu.colostate.netsec.module;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import edu.colostate.netsec.BgpmonOuterClass;

public abstract class Module {
    protected String moduleId;
    protected Logger logger;
    private Map<String, Module> modules;
    private Timer timer;
    
    public Module(String moduleId, Map<String, Module> modules, String logDirectory) {
        this.moduleId = moduleId;
        this.modules = modules;

        logger = Logger.getLogger(moduleId);
        try {
            FileHandler fileHandler = new FileHandler(logDirectory + "/prefix-hijack-" + moduleId + ".log");
            logger.addHandler(fileHandler);
            SimpleFormatter formatter = new SimpleFormatter();
            fileHandler.setFormatter(formatter);
        } catch(Exception e) {
            e.printStackTrace();
            //TODO throw e;
        }
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
