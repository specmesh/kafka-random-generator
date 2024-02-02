/*
 * Copyright 2023 SpecMesh Contributors (https://github.com/specmesh)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.specmesh.avro.random.generator;

import org.apache.avro.generic.GenericRecord;

import java.util.Random;
import java.util.function.BiConsumer;

public class API {

    private final int count;
    private final String keyField;
    private final Generator generator;

    public API(final int count, final String keyField, final String schema) {
        this.count = count;
        this.keyField = keyField;
        var generatorBuilder = new Generator.Builder()
                .random(new Random(System.currentTimeMillis()))
                .generation(count)
                .schemaString(schema);

        generator = generatorBuilder.build();
    }

    public void run(BiConsumer<Object, GenericRecord> consumer) {

        for (int i = 0; i < count; i++) {
            final var generatedObject = generator.generate();
            if (!(generatedObject instanceof GenericRecord)) {
                throw new RuntimeException(String.format(
                        "Expected Avro Random Generator to return instance of GenericRecord, found %s instead",
                        generatedObject.getClass().getName()
                ));
            }
            final var avroRecord = (GenericRecord) generatedObject;

            final var key = avroRecord.get(keyField);
            if (key == null) {
                throw new RuntimeException(String.format(
                        "Expected key not found:" + keyField +
                        generatedObject.getClass().getName()
                ));
            }
            consumer.accept(key, avroRecord);
        }
    }
}
