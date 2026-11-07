/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/**
 * Serializer for {@link LanceSplit}.
 *
 * <p>Encoding format (version=1): {@code [kind:byte][fragmentId:long][splitId:utf]}.
 * The byte enum encoding avoids the space waste of writing the string enum name; splitId is written
 * separately to keep it independent of kind, making it easy to extend other split semantics in the
 * future without changing the old-version decoding.
 */
public final class LanceSplitSerializer implements SimpleVersionedSerializer<LanceSplit> {

    public static final LanceSplitSerializer INSTANCE = new LanceSplitSerializer();

    private static final int VERSION = 1;

    private static final byte KIND_FRAGMENT = 0;
    private static final byte KIND_WHOLE_DATASET = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LanceSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeByte(encodeKind(split.getKind()));
        out.writeLong(split.getFragmentId());
        out.writeUTF(split.splitId());
        return out.getCopyOfBuffer();
    }

    @Override
    public LanceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported LanceSplit serializer version: " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        LanceSplit.Kind kind = decodeKind(in.readByte());
        long fragmentId = in.readLong();
        String splitId = in.readUTF();
        return new LanceSplit(splitId, kind, fragmentId);
    }

    private static byte encodeKind(LanceSplit.Kind kind) {
        switch (kind) {
            case FRAGMENT:
                return KIND_FRAGMENT;
            case WHOLE_DATASET:
                return KIND_WHOLE_DATASET;
            default:
                throw new IllegalArgumentException("Unknown kind: " + kind);
        }
    }

    private static LanceSplit.Kind decodeKind(byte b) throws IOException {
        switch (b) {
            case KIND_FRAGMENT:
                return LanceSplit.Kind.FRAGMENT;
            case KIND_WHOLE_DATASET:
                return LanceSplit.Kind.WHOLE_DATASET;
            default:
                throw new IOException("Unknown kind byte: " + b);
        }
    }
}
