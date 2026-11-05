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
import java.util.ArrayList;
import java.util.List;

/**
 * {@link LanceEnumState} 的序列化器：复用 {@link LanceSplitSerializer} 作为元素编码。
 *
 * <p>编码格式（version=1）：{@code [count:int][elementVersion:int][len:int][bytes]...}。
 * 单独写出元素版本号便于后续 split 结构演进时的向后兼容。
 */
public final class LanceEnumStateSerializer implements SimpleVersionedSerializer<LanceEnumState> {

    public static final LanceEnumStateSerializer INSTANCE = new LanceEnumStateSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LanceEnumState state) throws IOException {
        List<LanceSplit> splits = state.getRemainingSplits();
        DataOutputSerializer out = new DataOutputSerializer(64 + splits.size() * 32);
        out.writeInt(splits.size());
        out.writeInt(LanceSplitSerializer.INSTANCE.getVersion());
        for (LanceSplit split : splits) {
            byte[] bytes = LanceSplitSerializer.INSTANCE.serialize(split);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public LanceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported LanceEnumState serializer version: " + version);
        }
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        int count = in.readInt();
        int elementVersion = in.readInt();
        List<LanceSplit> splits = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            splits.add(LanceSplitSerializer.INSTANCE.deserialize(elementVersion, bytes));
        }
        return new LanceEnumState(splits);
    }
}
