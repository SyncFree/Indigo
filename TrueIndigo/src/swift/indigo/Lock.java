package swift.indigo;

import swift.crdt.LockType;

public class Lock {

    public String id;
    public LockType type;

    Lock() {
    }

    public Lock(String id, LockType type) {
        this.id = id;
        this.type = type;
    }

    public String id() {
        return id;
    }

    public LockType type() {
        return type;
    }

    public String toString() {
        return String.format("<%s : %s>", id, type);
    }

    public int hashCode() {
        return type.hashCode() ^ id.hashCode();
    }

    private boolean equals(Lock other) {
        return type.equals(other.type) && id.equals(other.id);
    }

    public boolean equals(Object other) {
        return other != null && equals((Lock) other);
    }
}
