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

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Replace with PicoCLi - Find a good argument parser that doesn't strip double quotes off of arguments and allows for
 * mutually exclusive options to cancel each other out without error
 * */
public final class Main {
  private Main() {

  }
  public static final String PROGRAM_NAME = "arg";

  public static final String SCHEMA_SHORT_FLAG = "-s";
  public static final String SCHEMA_LONG_FLAG = "--schema";

  public static final String SCHEMA_FILE_SHORT_FLAG = "-f";
  public static final String SCHEMA_FILE_LONG_FLAG = "--schema-file";

  public static final String PRETTY_SHORT_FLAG = "-p";
  public static final String PRETTY_LONG_FLAG = "--pretty";

  public static final String COMPACT_SHORT_FLAG = "-c";
  public static final String COMPACT_LONG_FLAG = "--compact";

  public static final String JSON_SHORT_FLAG = "-j";
  public static final String JSON_LONG_FLAG = "--json";

  public static final String BINARY_SHORT_FLAG = "-b";
  public static final String BINARY_LONG_FLAG = "--binary";

  public static final String ITERATIONS_SHORT_FLAG = "-i";
  public static final String ITERATIONS_LONG_FLAG = "--iterations";

  public static final String OUTPUT_FILE_SHORT_FLAG = "-o";
  public static final String OUTPUT_FILE_LONG_FLAG = "--output";

  public static final String HELP_SHORT_FLAG_1 = "-?";
  public static final String HELP_SHORT_FLAG_2 = "-h";
  public static final String HELP_LONG_FLAG = "--help";

  private static final boolean PRETTY_FORMAT = true;
  private static final boolean COMPACT_FORMAT = false;

  private static final boolean JSON_ENCODING = true;
  private static final boolean BINARY_ENCODING = false;

  /**
   * Parses options passed in via the args argument to main() and then leverages a new
   * {@link Generator} object to produce randomized output according to the parsed options.
   * @param args - args
   */
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:JavaNCSS"})
  public static void main(String[] args) {
    String schema = null;
    String schemaFile = "-";

    boolean jsonFormat = PRETTY_FORMAT;

    boolean encoding = JSON_ENCODING;

    long iterations = 1;
    String outputFile = null;

    Iterator<String> argv = Arrays.asList(args).iterator();
    while (argv.hasNext()) {
      String flag = argv.next();
      switch (flag) {
        case SCHEMA_SHORT_FLAG:
        case SCHEMA_LONG_FLAG:
          schemaFile = null;
          schema = nextArg(argv, flag);
          break;
        case SCHEMA_FILE_SHORT_FLAG:
        case SCHEMA_FILE_LONG_FLAG:
          schema = null;
          schemaFile = nextArg(argv, flag);
          break;
        case PRETTY_SHORT_FLAG:
        case PRETTY_LONG_FLAG:
          jsonFormat = PRETTY_FORMAT;
          break;
        case COMPACT_SHORT_FLAG:
        case COMPACT_LONG_FLAG:
          jsonFormat = COMPACT_FORMAT;
          break;
        case JSON_SHORT_FLAG:
        case JSON_LONG_FLAG:
          encoding = JSON_ENCODING;
          break;
        case BINARY_SHORT_FLAG:
        case BINARY_LONG_FLAG:
          encoding = BINARY_ENCODING;
          break;
        case ITERATIONS_SHORT_FLAG:
        case ITERATIONS_LONG_FLAG:
          iterations = parseIterations(nextArg(argv, flag), flag);
          break;
        case OUTPUT_FILE_SHORT_FLAG:
        case OUTPUT_FILE_LONG_FLAG:
          outputFile = nextArg(argv, flag);
          break;
        case HELP_SHORT_FLAG_1:
        case HELP_SHORT_FLAG_2:
        case HELP_LONG_FLAG:
          usage();
          break;
        default:
          System.err.printf("%s: %s: unrecognized option%n%n", PROGRAM_NAME, flag);
          usage(1);
      }
    }

    Generator generator = null;
    try {
      generator = getGenerator(schema, schemaFile);
    } catch (IOException ioe) {
      System.err.println("Error occurred while trying to read schema file");
      System.exit(1);
    }

    DatumWriter<Object> dataWriter = new GenericDatumWriter<>(generator.schema());
    if (encoding == JSON_ENCODING) {
      try (OutputStream output = getOutput(outputFile)) {
        Encoder encoder = EncoderFactory.get().jsonEncoder(generator.schema(), output, jsonFormat);
        for (int i = 0; i < iterations; i++) {
          dataWriter.write(generator.generate(), encoder);
        }
        encoder.flush();
        output.write('\n');
      } catch (IOException ioe) {
        System.err.println(
            "Error occurred while trying to write to output file: " + ioe.getLocalizedMessage()
        );
        System.exit(1);
      }
    } else {
      try (DataFileWriter<Object> dataFileWriter =
               new DataFileWriter<>(dataWriter).create(generator.schema(), getOutput(outputFile))) {
        for (int i = 0; i < iterations; i++) {
          dataFileWriter.append(generator.generate());
        }
      } catch (IOException ioe) {
        System.err.println(
            "Error occurred while trying to write to output file: " + ioe.getLocalizedMessage()
        );
        System.exit(1);
      }
    }
  }

  private static long parseIterations(String arg, String flag) {
    try {
      long result = Long.parseLong(arg);
      if (result < 0) {
        System.err.printf("%s: %s: argument cannot be negative%n", PROGRAM_NAME, flag);
        usage(1);
      }
      return result;
    } catch (NumberFormatException nfe) {
      System.err.printf("%s: %s: argument must be a number%n", PROGRAM_NAME, flag);
      usage(1);
    }
    System.err.println(
        "This statement was put in to make the compiler happy."
        + " If you are seeing it, something has gone very wrong."
    );
    System.exit(1);
    return 0L;
  }

  private static String nextArg(Iterator<String> argv, String flag) {
    if (!argv.hasNext()) {
      System.err.printf("%s: %s: argument required%n", PROGRAM_NAME, flag);
      usage(1);
    }
    return argv.next();
  }

  private static void usage() {
    usage(0);
  }

  private static void usage(int exitValue) {
    String header = String.format("%s: Generate random Avro data%n", PROGRAM_NAME);

    String summary = String.format(
        "Usage: %s [%s <file> | %s <schema>] [%s | %s] [%s | %s] [%s <i>] [%s <file>]%n%n",
        PROGRAM_NAME,
        SCHEMA_FILE_SHORT_FLAG,
        SCHEMA_SHORT_FLAG,
        JSON_SHORT_FLAG,
        BINARY_SHORT_FLAG,
        PRETTY_SHORT_FLAG,
        COMPACT_SHORT_FLAG,
        ITERATIONS_SHORT_FLAG,
        OUTPUT_FILE_SHORT_FLAG
    );

    final String indentation = "    ";
    final String separation = "\t";
    String flags =
        "Flags:\n"
        + String.format(
            "%s%s, %s, %s:%s%s%n",
            indentation,
            HELP_SHORT_FLAG_1,
            HELP_SHORT_FLAG_2,
            HELP_LONG_FLAG,
            separation,
            "Print a brief usage summary and exit with status 0"
        ) + String.format(
            "%s%s, %s:%s%s%n",
            indentation,
            BINARY_SHORT_FLAG,
            BINARY_LONG_FLAG,
            separation,
            "Encode outputted data in binary format"
        ) + String.format(
            "%s%s, %s:%s%s%n",
            indentation,
            COMPACT_SHORT_FLAG,
            COMPACT_LONG_FLAG,
            separation,
            "Output each record on a single line of its own (has no effect if encoding is not JSON)"
        ) + String.format(
            "%s%s <file>, %s <file>:%s%s%n",
            indentation,
            SCHEMA_FILE_SHORT_FLAG,
            SCHEMA_FILE_LONG_FLAG,
            separation,
            "Read the schema to spoof from <file>, or stdin if <file> is '-' (default is '-')"
        ) + String.format(
            "%s%s <i>, %s <i>:%s%s%n",
            indentation,
            ITERATIONS_SHORT_FLAG,
            ITERATIONS_LONG_FLAG,
            separation,
            "Output <i> iterations of spoofed data (default is 1)"
        ) + String.format(
            "%s%s, %s:%s%s%n",
            indentation,
            JSON_SHORT_FLAG,
            JSON_LONG_FLAG,
            separation,
            "Encode outputted data in JSON format (default)"
        ) + String.format(
            "%s%s <file>, %s <file>:%s%s%n",
            indentation,
            OUTPUT_FILE_SHORT_FLAG,
            OUTPUT_FILE_LONG_FLAG,
            separation,
            "Write data to the file <file>, or stdout if <file> is '-' (default is '-')"
        ) + String.format(
            "%s%s, %s:%s%s%n",
            indentation,
            PRETTY_SHORT_FLAG,
            PRETTY_LONG_FLAG,
            separation,
            "Output each record in prettified format (has no effect if encoding is not JSON)"
              + "(default)"
        ) + String.format(
            "%s%s <schema>, %s <schema>:%s%s%n",
            indentation,
            SCHEMA_SHORT_FLAG,
            SCHEMA_LONG_FLAG,
            separation,
            "Spoof the schema <schema>"
        ) + "\n";

    String footer = String.format(
        "%s%n%s%n",
        "Source repository:",
        "https://github.com/specmesh/kafka-random-generator"
    );

    System.err.printf(header + summary + flags + footer);
    System.exit(exitValue);
  }

  private static Generator getGenerator(String schema, String schemaFile) throws IOException {
    if (schema != null) {
      return new Generator.Builder().schemaString(schema).build();
    } else if (!schemaFile.equals("-")) {
      return new Generator.Builder().schemaFile(new File(schemaFile)).build();
    } else {
      System.err.println("Reading schema from stdin...");
      return new Generator.Builder().schemaStream(System.in).build();
    }
  }

  private static OutputStream getOutput(String outputFile) throws IOException {
    if (outputFile != null && !outputFile.equals("-")) {
      return new FileOutputStream(outputFile);
    } else {
      return System.out;
    }
  }
}
