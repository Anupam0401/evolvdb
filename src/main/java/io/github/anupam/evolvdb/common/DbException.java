package io.github.anupam.evolvdb.common;

/** Base runtime exception for EvolvDB. */
public class DbException extends RuntimeException {
    public DbException(String message) { super(message); }
    public DbException(String message, Throwable cause) { super(message, cause); }
}
