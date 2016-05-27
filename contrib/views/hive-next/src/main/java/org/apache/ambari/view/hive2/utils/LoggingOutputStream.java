package org.apache.ambari.view.hive2.utils;

import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class LoggingOutputStream extends OutputStream {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    private final Logger logger;
    private final LogLevel level;

    public enum LogLevel {
        TRACE, DEBUG, INFO, WARN, ERROR,
    }

    public LoggingOutputStream(Logger logger, LogLevel level) {
        this.logger = logger;
        this.level = level;
    }

    @Override
    public void write(int b) {
        if (b == '\n') {
            String line = baos.toString();
            baos.reset();

            switch (level) {
                case TRACE:
                    logger.trace(line);
                    break;
                case DEBUG:
                    logger.debug(line);
                    break;
                case ERROR:
                    logger.error(line);
                    break;
                case INFO:
                    logger.info(line);
                    break;
                case WARN:
                    logger.warn(line);
                    break;
            }
        } else {
            baos.write(b);
        }
    }


    /**
     * Closes this output stream and releases any system resources
     * associated with this stream. The general contract of <code>close</code>
     * is that it closes the output stream. A closed stream cannot perform
     * output operations and cannot be reopened.
     * <p/>
     * The <code>close</code> method of <code>OutputStream</code> does nothing.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void close() throws IOException {
        baos.close();
    }
}
