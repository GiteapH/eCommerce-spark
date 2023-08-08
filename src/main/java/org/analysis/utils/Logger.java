package org.analysis.utils;

public class Logger {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Logger.class);
    public static org.apache.log4j.Logger getInstance(){
       return logger;
    }

    public static void main(String[] args) {
        logger.error("1516156");
    }
}
