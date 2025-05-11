package io.bewaremypower.github.ml;

import org.apache.bookkeeper.mledger.Position;

record PositionImpl(long streamId, long baseOffset, long numMessages) implements Position {

    @Override
    public long getLedgerId() {
        return streamId;
    }

    @Override
    public long getEntryId() {
        return baseOffset;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Position other) {
            return getLedgerId() == other.getLedgerId() && getEntryId() == other.getEntryId();
        }
        return false;
    }
}
