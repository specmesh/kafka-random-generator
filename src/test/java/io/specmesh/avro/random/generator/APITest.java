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

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


public class APITest {


    @Test
    public void shouldGenerateAndCallConsumer() {

        int count1 = 10;
        final var api = new API(count1, "key", schema);

        final var counter = new AtomicInteger();

        api.run((key, genericRecord) -> {
            counter.incrementAndGet();
            System.out.println(key);
        });

        assertThat(counter.get(), is(count1));
    }

    private static final String schema = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"simple_schema\",\n" +
            "  \"namespace\": \"io.specmesh.avro.random.generator\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"key\",\n" +
            "        \"type\": {\n" +
            "          \"type\": \"string\",\n" +
            "          \"arg.properties\": {\n" +
            "            \"regex\": \"[a-zA-Z]*\",\n" +
            "            \"length\": 10\n" +
            "          }\n" +
            "        }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"number_length_property\",\n" +
            "      \"type\":\n" +
            "      {\n" +
            "        \"type\": \"string\",\n" +
            "        \"arg.properties\": {\n" +
            "          \"regex\": \"[a-zA-Z]*\",\n" +
            "          \"length\": 10\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"MY_STRING\",\n" +
            "      \"type\": {\n" +
            "        \"type\": \"string\",\n" +
            "        \"arg.properties\": {\n" +
            "          \"regex\": \"[a-zA-Z0-9]{16}\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"default\": \"\"\n" +
            "    }\n" +
            "  ]\n" +
            "}\n";
}
