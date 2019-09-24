package com.dataflow.lefty;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by Owen Rees-Hayward on 31/10/2016.
 */
public final class HashShort extends Datum {

    private static long offset = 0;

    private static final long valueOffset = offset += 0;

    private static final long objectSize = offset += 8;

    private long objectOffset;

    @Override
    public final long getObjectSize() {
        return objectSize;
    }

    @Override
    public final void setObjectOffset(final long objectOffset) {
        this.objectOffset = objectOffset;
    }

    public final long getValue() {
        return unsafe.getLong(objectOffset + valueOffset);
    }

    public final void setValue(final long newValue) {
        unsafe.putLong(objectOffset + valueOffset, newValue);
    }

    @Override
    public final Datum get(final long address, final int index) {
        final long offset = address + (index * getObjectSize());
        setObjectOffset(offset);
        return this;
    }

    @Override
    public void store(final ByteBuffer buffer) {
        buffer.putLong(getValue());
    }

    @Override
    public void load(final ByteBuffer buffer) {
        final long dataLong = buffer.getLong();
        setValue(dataLong);
    }

    @Override
    public void load(final DataInputStream is) throws IOException {
        final long dataLong = is.readLong();
        setValue(dataLong);
    }
}
