package demo.hadoop.log;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLog {

	private static final Logger logger = LoggerFactory.getLogger(TestLog.class);
    public static void main(String[] args) {
    	PropertyConfigurator.configure("src/log4j.properties");
        logger.debug("This is debug message");
        logger.info("This is info message");
        logger.warn("This is warn message");
        logger.error("This is error message");
    }
}