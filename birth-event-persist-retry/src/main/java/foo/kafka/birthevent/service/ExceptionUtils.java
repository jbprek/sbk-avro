package foo.kafka.birthevent.service;

import org.springframework.dao.TransientDataAccessException;

import java.sql.SQLTransientException;

public final class ExceptionUtils {

    private ExceptionUtils() {}

    public static boolean isTransient(Throwable t) {
        while (t != null) {
            // Spring's TransientDataAccessException
            if (t instanceof TransientDataAccessException) {
                return true;
            }
            // JDBC's SQLTransientException and subclasses
            if (t instanceof SQLTransientException) {
                return true;
            }
            // Some drivers wrap transient SQL exceptions in generic SQLExceptions or other wrappers.
            // Inspect class name for common 'Transient' indicator as a fallback.
            if (t instanceof java.sql.SQLException) {
                String cls = t.getClass().getSimpleName();
                if (cls.toLowerCase().contains("transient")) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }
}

