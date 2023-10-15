package simpleDB.backend.dm.page;

public interface Page {
    void lock();
    void unlock();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
