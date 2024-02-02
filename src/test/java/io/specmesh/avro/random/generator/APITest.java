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