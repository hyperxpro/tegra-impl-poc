package org.tegra.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Binary serializer interface for Tegra types.
 * <p>
 * Implementations provide compact binary encoding for disk eviction (pART subtrees)
 * and network transport (GAS messages, graph data).
 *
 * @param <T> the type to serialize/deserialize
 */
public interface TegraSerializer<T> {

    /**
     * Serializes the given value to the output stream.
     *
     * @param value the value to serialize
     * @param out   the data output stream
     * @throws IOException if an I/O error occurs
     */
    void serialize(T value, DataOutput out) throws IOException;

    /**
     * Deserializes a value from the input stream.
     *
     * @param in the data input stream
     * @return the deserialized value
     * @throws IOException if an I/O error occurs
     */
    T deserialize(DataInput in) throws IOException;

    /**
     * Estimates the serialized size in bytes. Used for buffer pre-allocation.
     *
     * @param value the value to estimate
     * @return estimated size in bytes
     */
    int estimateSize(T value);
}
