package io.github.anupam.evolvdb.storage.page;

/**
 * Thrown when a page does not have enough free space to accommodate an insert, even after
 * compaction. Callers (e.g. HeapFile) can catch this specifically to try the next page without
 * swallowing unrelated exceptions.
 */
public final class PageFullException extends RuntimeException {

    public PageFullException(int requiredBytes, int availableBytes) {
        super(
                "Page full: need "
                        + requiredBytes
                        + " bytes but only "
                        + availableBytes
                        + " available");
    }
}
