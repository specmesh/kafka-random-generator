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

import com.mifmif.common.regex.Generex;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

/**
 * Generates Java objects according to an {@link Schema Avro Schema}.
 */
@SuppressWarnings("WeakerAccess")
public class Generator {

  private final Map<Schema, Generex> generexCache = new HashMap<>();
  private final Map<Schema, List<Object>> optionsCache = new HashMap<>();
  private final Map<Schema, Iterator<Object>> iteratorCache = new IdentityHashMap<>();

  /**
   * The name to use for the top-level JSON property when specifying ARG-specific attributes.
   */
  public static final String ARG_PROPERTIES_PROP = "arg.properties";

  /**
   * The name of the attribute for specifying length for supported schemas. Can be given as either
   * an integral number or an object with at least one of {@link #LENGTH_PROP_MIN} or
   * {@link #LENGTH_PROP_MAX} specified.
   */
  public static final String LENGTH_PROP = "length";
  /**
   * The name of the attribute for specifying the minimum length a generated value should have.
   * Must be given as an integral number greater than or equal to zero.
   */
  public static final String LENGTH_PROP_MIN = "min";
  /**
   * The name of the attribute for specifying the maximum length a generated value should have.
   * Must be given as an integral number strictly greater than the value given for
   * {@link #LENGTH_PROP_MIN}, or strictly greater than zero if none is specified.
   */
  public static final String LENGTH_PROP_MAX = "max";

  /**
   * The name of the attribute for specifying a regex that generated values should adhere to. Can
   * be used in conjunction with {@link #LENGTH_PROP}. Must be given as a string.
   */
  public static final String REGEX_PROP = "regex";

  /**
   * The name of the attribute for specifying a prefix that generated values should begin with. Will
   * be prepended to the beginning of any string values generated. Can be used in conjunction with
   * {@link #SUFFIX_PROP}.
   */
  public static final String PREFIX_PROP = "prefix";

  /**
   * The name of the attribute for specifying a suffix that generated values should end with. Will
   * be appended to the end of any string values generated. Can be used in conjunction with
   * {@link #PREFIX_PROP}.
   */
  public static final String SUFFIX_PROP = "suffix";

  /**
   * The name of the attribute for specifying specific values which should be randomly chosen from
   * when generating values for the schema. Can be given as either an array of values or an object
   * with both {@link #OPTIONS_PROP_FILE} and {@link #OPTIONS_PROP_ENCODING}
   * specified.
   */
  public static final String OPTIONS_PROP = "options";
  /**
   * The name of a file from which to read specific values to generate for the given schema. Must
   * be given as a string.
   */
  public static final String OPTIONS_PROP_FILE = "file";
  /**
   * The encoding of the options file; currently only "binary" and "json" are supported. Must be
   * given as a string.
   */
  public static final String OPTIONS_PROP_ENCODING = "encoding";

  /**
   * The name of the attribute for specifying special properties for keys in map schemas. Since
   * all Avro maps have keys of type string, no schema is supplied to specify key attributes; this
   * special keys attribute takes its place. Must be given as an object.
   */
  public static final String KEYS_PROP = "keys";

  /**
   * The name of the attribute for specifying a possible range of values for numeric types. Must be
   * given as an object.
   */
  public static final String RANGE_PROP = "range";
  /**
   * The name of the attribute for specifying the (inclusive) minimum value in a range. Must be
   * given as a numeric type that is integral if the given schema is as well.
   */
  public static final String RANGE_PROP_MIN = "min";
  /**
   * The name of the attribute for specifying the (exclusive) maximum value in a range. Must be
   * given as a numeric type that is integral if the given schema is as well.
   */
  public static final String RANGE_PROP_MAX = "max";

  /**
   * The name of the attribute for specifying the likelihood that the value true is generated for a
   * boolean schema. Must be given as a floating type in the range [0.0, 1.0].
   */
  public static final String ODDS_PROP = "odds";

  /**
   * The name of the attribute for specifying iterative behavior for generated values. Must be
   * given as an object with at least the {@link #ITERATION_PROP_START} property specified. The
   * first generated value for the schema will then be equal to the value given for
   * {@link #ITERATION_PROP_START}, and successive values will increment by the value given for
   * {@link #ITERATION_PROP_STEP} (or its default, if no value is given), wrapping around at the
   * value given for {@link #ITERATION_PROP_RESTART} (or its default, if no value is given).
   */
  public static final String ITERATION_PROP = "iteration";
  /**
   * The name of the attribute for specifying the first value in a schema with iterative
   * generation. Must be given as a numeric type that is integral if the given schema is as well.
   */
  public static final String ITERATION_PROP_START = "start";
  /**
   * The name of the attribute for specifying the wraparound value in a schema with iterative
   * generation. If given, must be a numeric type that is integral if the given schema is as well.
   * If not given, defaults to the maximum possible value for the schema type if the value for
   * {@link #ITERATION_PROP_STEP} is positive, or the minimum possible value for the schema type if
   * the value for {@link #ITERATION_PROP_STEP} is negative.
   */
  public static final String ITERATION_PROP_RESTART = "restart";
  /**
   * The name of the attribute for specifying the increment value in a schema with iterative
   * generation. If given, must be a numeric type that is integral if the given schema is as well.
   * If not given, defaults to 1 if the value for {@link #ITERATION_PROP_RESTART} is greater than
   * the value for {@link #ITERATION_PROP_START}, and -1 if the value for
   * {@link #ITERATION_PROP_RESTART} is less than the value for {@link #ITERATION_PROP_START}.
   */
  public static final String ITERATION_PROP_STEP = "step";
  /**
   * The name of the attribute for specifying the initial value in a schema with iterative
   * generation. If given, must be a numeric type that is integral if the given schema is as well.
   * If not given, defaults to the value for {@link #ITERATION_PROP_START}.
   */
  public static final String ITERATION_PROP_INITIAL = "initial";

  static final String DECIMAL_LOGICAL_TYPE_NAME = "decimal";

  private final Schema topLevelSchema;
  private final Random random;
  private final long generation;

  /**
   * Creates a generator out of an already-parsed {@link Schema}.
   * @param topLevelSchema The schema to generate values for.
   * @param random The object to use for generating randomness when producing values.
   */
  @Deprecated
  public Generator(Schema topLevelSchema, Random random) {
    this(topLevelSchema, random, 0L);
  }

  protected Generator(Schema topLevelSchema, Random random, long generation) {
    this.topLevelSchema = topLevelSchema;
    this.random = random;
    this.generation = generation;
  }

  /**
   * Creates a generator out of the yet-to-be-parsed Schema string.
   * @param schemaString An Avro Schema represented as a string.
   * @param random The object to use for generating randomness when producing values.
   */
  @Deprecated
  public Generator(String schemaString, Random random) {
    this(new Schema.Parser().parse(schemaString), random);
  }

  /**
   * Reads in a schema, parses it, and creates a generator for it.
   * @param schemaStream The stream that the schema is read from.
   * @param random The object to use for generating randomness when producing values.
   * @throws IOException if an error occurs while reading from the input stream.
   */
  @Deprecated
  public Generator(InputStream schemaStream, Random random) throws IOException {
    this(new Schema.Parser().parse(schemaStream), random);
  }

  /**
   * Reads in a schema, parses it, and creates a generator for it.
   * @param schemaFile The file that contains the schema to generate values for.
   * @param random The object to use for generating randomness when producing values.
   * @throws IOException if an error occurs while reading from the schema file.
   */
  @Deprecated
  public Generator(File schemaFile, Random random) throws IOException {
    this(new Schema.Parser().parse(schemaFile), random);
  }

  public static class Builder {

    private Schema topLevelSchema;
    private Random random;
    private long generation;
    private Schema.Parser parser;

    public Builder() {
      parser = new Schema.Parser();
      random = new Random();
      generation = 0L;
    }

    public Builder schema(Schema schema) {
      topLevelSchema = schema;
      return this;
    }

    public Builder schemaFile(File schemaFile) throws IOException {
      topLevelSchema = parser.parse(schemaFile);
      return this;
    }

    public Builder schemaStream(InputStream schemaStream) throws IOException {
      topLevelSchema = parser.parse(schemaStream);
      return this;
    }

    public Builder schemaString(String schemaString) {
      topLevelSchema = parser.parse(schemaString);
      return this;
    }

    public Builder random(Random random) {
      this.random = random;
      return this;
    }

    public Builder generation(long generation) {
      this.generation = generation;
      return this;
    }

    public Generator build() {
      return new Generator(topLevelSchema, random, generation);
    }
  }

  /**
   * @return The schema that the generator produces values for.
   */
  public Schema schema() {
    return topLevelSchema;
  }

  /**
   * Generate an object that matches the given schema and its specified properties.
   * @return An object whose type corresponds to the top-level schema as follows:
   * <table summary="Schema Type-to-Java class specifications">
   *   <tr>
   *     <th>Schema Type</th>
   *     <th>Java Class</th>
   *   </tr>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#ARRAY ARRAY}</td>
   *     <td>{@link Collection}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#BOOLEAN BOOLEAN}</td>
   *     <td>{@link Boolean}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#BYTES BYTES}</td>
   *     <td>{@link ByteBuffer}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#DOUBLE DOUBLE}</td>
   *     <td>{@link Double}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#ENUM ENUM}</td>
   *     <td>{@link GenericEnumSymbol}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#FIXED FIXED}</td>
   *     <td>{@link GenericFixed}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#FLOAT FLOAT}</td>
   *     <td>{@link Float}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#INT INT}</td>
   *     <td>{@link Integer}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#LONG LONG}</td>
   *     <td>{@link Long}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#MAP MAP}</td>
   *     <td>
   *       {@link Map}&lt;{@link String}, V&gt; where V is the corresponding Java class for the
   *       Avro map's values
   *     </td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#NULL NULL}</td>
   *     <td>{@link Object} (but will always be null)</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#RECORD RECORD}</td>
   *     <td>{@link GenericRecord}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#STRING STRING}</td>
   *     <td>{@link String}</td>
   *   <tr>
   *     <td>{@link org.apache.avro.Schema.Type#UNION UNION}</td>
   *     <td>
   *       The corresponding Java class for whichever schema is chosen to be generated out of the
   *       ones present in the given Avro union.
   *     </td>
   * </table>
   */
  public Object generate() {
    return generateObject(topLevelSchema);
  }

  private Object generateObject(Schema schema) {
    Map propertiesProp = getProperties(schema).orElse(Collections.emptyMap());
    if (propertiesProp.containsKey(OPTIONS_PROP)) {
      return generateOption(schema, propertiesProp);
    }
    if (propertiesProp.containsKey(ITERATION_PROP)) {
      return generateIteration(schema, propertiesProp);
    }
    switch (schema.getType()) {
      case ARRAY:
        return generateArray(schema, propertiesProp);
      case BOOLEAN:
        return generateBoolean(propertiesProp);
      case BYTES:
        return generateBytes(schema, propertiesProp);
      case DOUBLE:
        return generateDouble(propertiesProp);
      case ENUM:
        return generateEnumSymbol(schema);
      case FIXED:
        return generateFixed(schema);
      case FLOAT:
        return generateFloat(propertiesProp);
      case INT:
        return generateInt(propertiesProp);
      case LONG:
        return generateLong(propertiesProp);
      case MAP:
        return generateMap(schema, propertiesProp);
      case NULL:
        return generateNull();
      case RECORD:
        return generateRecord(schema);
      case STRING:
        return generateString(schema, propertiesProp);
      case UNION:
        return generateUnion(schema);
      default:
        throw new RuntimeException("Unrecognized schema type: " + schema.getType());
    }
  }

  private Optional<Map> getProperties(Schema schema) {
    Object propertiesProp = schema.getObjectProp(ARG_PROPERTIES_PROP);
    if (propertiesProp == null) {
      return Optional.empty();
    } else if (propertiesProp instanceof Map) {
      return Optional.of((Map) propertiesProp);
    } else {
      throw new RuntimeException(String.format(
          "%s property must be given as object, was %s instead",
          ARG_PROPERTIES_PROP,
          propertiesProp.getClass().getName()
      ));
    }
  }

  private <L extends LogicalType> L getLogicalType(
      Schema schema,
      String logicalTypeName,
      Class<L> logicalTypeClass
  ) {
    return Optional.ofNullable(schema.getLogicalType())
        .filter(logicalType -> Objects.equals(logicalTypeName, logicalType.getName()))
        .map(logicalTypeClass::cast)
        .orElse(null);
  }

  private LogicalTypes.Decimal getDecimalLogicalType(Schema schema) {
    return getLogicalType(schema, DECIMAL_LOGICAL_TYPE_NAME, LogicalTypes.Decimal.class);
  }

  private void enforceMutualExclusion(
      Map propertiesProp,
      String includedProp,
      String... excludedProps) {
    for (String excludedProp : excludedProps) {
      if (propertiesProp.containsKey(excludedProp)) {
        throw new RuntimeException(String.format(
            "Cannot specify %s prop when %s prop is given",
            excludedProp,
            includedProp
        ));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Object wrapOption(Schema schema, Object option) {
    if (schema.getType() == Schema.Type.BYTES && option instanceof String) {
      option = ByteBuffer.wrap(((String) option).getBytes(Charset.defaultCharset()));
    } else if (schema.getType() == Schema.Type.FLOAT && option instanceof Double) {
      option = ((Double) option).floatValue();
    } else if (schema.getType() == Schema.Type.LONG && option instanceof Integer) {
      option = ((Integer) option).longValue();
    } else if (schema.getType() == Schema.Type.ARRAY && option instanceof Collection) {
      option = new GenericData.Array(schema, (Collection) option);
    } else if (schema.getType() == Schema.Type.ENUM && option instanceof String) {
      option = new GenericData.EnumSymbol(schema, (String) option);
    } else if (schema.getType() == Schema.Type.FIXED && option instanceof String) {
      option =
          new GenericData.Fixed(schema, ((String) option).getBytes(Charset.defaultCharset()));
    } else if (schema.getType() == Schema.Type.RECORD && option instanceof Map) {
      Map optionMap = (Map) option;
      GenericRecordBuilder optionBuilder = new GenericRecordBuilder(schema);
      for (Schema.Field field : schema.getFields()) {
        if (optionMap.containsKey(field.name())) {
          optionBuilder.set(field, optionMap.get(field.name()));
        }
      }
      option = optionBuilder.build();
    }
    return option;
  }

  @SuppressWarnings({"unchecked", "checkstyle:JavaNCSS"})
  private List<Object> parseOptions(Schema schema, Map propertiesProp) {
    enforceMutualExclusion(
        propertiesProp, OPTIONS_PROP,
        LENGTH_PROP, REGEX_PROP, ITERATION_PROP, RANGE_PROP
    );

    Object optionsProp = propertiesProp.get(OPTIONS_PROP);
    if (optionsProp instanceof Collection) {
      Collection optionsList = (Collection) optionsProp;
      if (optionsList.isEmpty()) {
        throw new RuntimeException(String.format(
            "%s property cannot be empty",
            OPTIONS_PROP
        ));
      }
      List<Object> options = new ArrayList<>();
      for (Object option : optionsList) {
        option = wrapOption(schema, option);
        if (!GenericData.get().validate(schema, option)) {
          throw new RuntimeException(String.format(
              "Invalid option for %s schema: type %s, value '%s'",
              schema.getType().getName(),
              option.getClass().getName(),
              option
          ));
        }
        options.add(option);
      }
      return options;
    } else if (optionsProp instanceof Map) {
      Map optionsProps = (Map) optionsProp;
      Object optionsFile = optionsProps.get(OPTIONS_PROP_FILE);
      if (optionsFile == null) {
        throw new RuntimeException(String.format(
            "%s property must contain '%s' field when given as object",
            OPTIONS_PROP,
            OPTIONS_PROP_FILE
        ));
      }
      if (!(optionsFile instanceof String)) {
        throw new RuntimeException(String.format(
            "'%s' field of %s property must be given as string, was %s instead",
            OPTIONS_PROP_FILE,
            OPTIONS_PROP,
            optionsFile.getClass().getName()
        ));
      }
      Object optionsEncoding = optionsProps.get(OPTIONS_PROP_ENCODING);
      if (optionsEncoding == null) {
        throw new RuntimeException(String.format(
            "%s property must contain '%s' field when given as object",
            OPTIONS_PROP,
            OPTIONS_PROP_FILE
        ));
      }
      if (!(optionsEncoding instanceof String)) {
        throw new RuntimeException(String.format(
            "'%s' field of %s property must be given as string, was %s instead",
            OPTIONS_PROP_ENCODING,
            OPTIONS_PROP,
            optionsEncoding.getClass().getName()
        ));
      }
      try (InputStream optionsStream = new FileInputStream((String) optionsFile)) {
        DatumReader<Object> optionReader = new GenericDatumReader(schema);
        Decoder decoder;
        if ("binary".equals(optionsEncoding)) {
          decoder = DecoderFactory.get().binaryDecoder(optionsStream, null);
        } else if ("json".equals(optionsEncoding)) {
          decoder = DecoderFactory.get().jsonDecoder(schema, optionsStream);
        } else {
          throw new RuntimeException(String.format(
              "'%s' field of %s property only supports two formats: 'binary' and 'json'",
              OPTIONS_PROP_ENCODING,
              OPTIONS_PROP
          ));
        }
        List<Object> options = new ArrayList<>();
        Object option = optionReader.read(null, decoder);
        while (option != null) {
          option = wrapOption(schema, option);
          if (!GenericData.get().validate(schema, option)) {
            throw new RuntimeException(String.format(
                "Invalid option for %s schema: type %s, value '%s'",
                schema.getType().getName(),
                option.getClass().getName(),
                option
            ));
          }
          options.add(option);
          try {
            option = optionReader.read(null, decoder);
          } catch (EOFException eofe) {
            break;
          }
        }
        return options;
      } catch (FileNotFoundException fnfe) {
        throw new RuntimeException(
            String.format(
                "Unable to locate options file '%s'",
                optionsFile
            ),
            fnfe
        );
      } catch (IOException ioe) {
        throw new RuntimeException(
            String.format(
                "Unable to read options file '%s'",
                optionsFile
            ),
            ioe
        );
      }
    } else {
      throw new RuntimeException(String.format(
          "%s prop must be an array or an object, was %s instead",
          OPTIONS_PROP,
          optionsProp.getClass().getName()
      ));
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T generateOption(Schema schema, Map propertiesProp) {
    if (!optionsCache.containsKey(schema)) {
      optionsCache.put(schema, parseOptions(schema, propertiesProp));
    }
    List<Object> options = optionsCache.get(schema);
    return (T) options.get(random.nextInt(options.size()));
  }

  private Iterator<Object> getBooleanIterator(Map iterationProps) {
    Object startProp = iterationProps.get(ITERATION_PROP_START);
    if (startProp == null) {
      throw new RuntimeException(String.format(
          "%s property must contain %s field",
          ITERATION_PROP,
          ITERATION_PROP_START
      ));
    }
    if (!(startProp instanceof Boolean)) {
      throw new RuntimeException(String.format(
          "%s field of %s property for a boolean schema must be a boolean, was %s instead",
          ITERATION_PROP_START,
          ITERATION_PROP,
          startProp.getClass().getName()
      ));
    }
    if (iterationProps.containsKey(ITERATION_PROP_RESTART)) {
      throw new RuntimeException(String.format(
          "%s property cannot contain %s field for a boolean schema",
          ITERATION_PROP,
          ITERATION_PROP_RESTART
      ));
    }
    if (iterationProps.containsKey(ITERATION_PROP_STEP)) {
      throw new RuntimeException(String.format(
          "%s property cannot contain %s field for a boolean schema",
          ITERATION_PROP,
          ITERATION_PROP_STEP
      ));
    }

    // If an odd number of records have been generated previously, then the boolean will have
    // changed state effectively once, and so the start state should be inverted.
    return new BooleanIterator((generation % 2 == 1) ^ ((Boolean) startProp));
  }

  @SuppressWarnings("checkstyle:JavaNCSS")
  private Iterator<Object> getIntegralIterator(
      Long iterationStartField,
      Long iterationRestartField,
      Long iterationStepField,
      Long iterationInitialField,
      IntegralIterator.Type type) {

    if (iterationStartField == null) {
      throw new RuntimeException(String.format(
          "%s property must contain %s field",
          ITERATION_PROP,
          ITERATION_PROP_START
      ));
    }

    long iterationStart = iterationStartField;
    long iterationRestart;
    long iterationStep;

    long restartHighDefault;
    long restartLowDefault;
    switch (type) {
      case INTEGER:
        restartHighDefault = Integer.MAX_VALUE;
        restartLowDefault = Integer.MIN_VALUE;
        break;
      case LONG:
        restartHighDefault = Long.MAX_VALUE;
        restartLowDefault = Long.MIN_VALUE;
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected IntegralIterator type: %s",
            type
        ));
    }

    if (iterationRestartField == null && iterationStepField == null) {
      iterationRestart = restartHighDefault;
      iterationStep = 1;
    } else if (iterationRestartField == null) {
      iterationStep = iterationStepField;
      if (iterationStep > 0) {
        iterationRestart = restartHighDefault;
      } else if (iterationStep < 0) {
        iterationRestart = -1 * restartLowDefault;
      } else {
        throw new RuntimeException(String.format(
            "%s field of %s property cannot be zero",
            ITERATION_PROP_STEP,
            ITERATION_PROP
        ));
      }
    } else if (iterationStepField == null) {
      iterationRestart = iterationRestartField;
      if (iterationRestart > iterationStart) {
        iterationStep = 1;
      } else if (iterationRestart < iterationStart) {
        iterationStep = -1;
      } else {
        throw new RuntimeException(String.format(
            "%s and %s fields of %s property cannot be equal",
            ITERATION_PROP_START,
            ITERATION_PROP_RESTART,
            ITERATION_PROP
        ));
      }
    } else {
      iterationRestart = iterationRestartField;
      iterationStep = iterationStepField;
      if (iterationStep == 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property cannot be zero",
            ITERATION_PROP_STEP,
            ITERATION_PROP
        ));
      }
      if (iterationStart == iterationRestart) {
        throw new RuntimeException(String.format(
            "%s and %s fields of %s property cannot be equal",
            ITERATION_PROP_START,
            ITERATION_PROP_RESTART,
            ITERATION_PROP
        ));
      }
      if (iterationRestart > iterationStart && iterationStep < 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property must be positive when %s field is greater than %s field",
            ITERATION_PROP_STEP,
            ITERATION_PROP,
            ITERATION_PROP_RESTART,
            ITERATION_PROP_START
        ));
      }
      if (iterationRestart < iterationStart && iterationStep > 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property must be negative when %s field is less than %s field",
            ITERATION_PROP_STEP,
            ITERATION_PROP,
            ITERATION_PROP_RESTART,
            ITERATION_PROP_START
        ));
      }
    }

    long iterationInitial = iterationStart;
    if (iterationInitialField != null) {
      iterationInitial = iterationInitialField;
    }

    return new IntegralIterator(
        iterationStart,
        iterationRestart,
        iterationStep,
        iterationInitial,
        generation,
        type
    );
  }

  @SuppressWarnings("checkstyle:JavaNCSS")
  private Iterator<Object> getDecimalIterator(
      Double iterationStartField,
      Double iterationRestartField,
      Double iterationStepField,
      Double iterationInitialField,
      DecimalIterator.Type type) {

    if (iterationStartField == null) {
      throw new RuntimeException(String.format(
          "%s property must contain %s field",
          ITERATION_PROP,
          ITERATION_PROP_START
      ));
    }

    double iterationStart = iterationStartField;
    double iterationRestart;
    double iterationStep;

    double restartHighDefault;
    double restartLowDefault;
    switch (type) {
      case FLOAT:
        restartHighDefault = Float.MAX_VALUE;
        restartLowDefault = -1 * Float.MAX_VALUE;
        break;
      case DOUBLE:
        restartHighDefault = Double.MAX_VALUE;
        restartLowDefault = -1 * Double.MAX_VALUE;
        break;
      default:
        throw new RuntimeException(String.format(
            "Unexpected DecimalIterator type: %s",
            type
        ));
    }

    if (iterationRestartField == null && iterationStepField == null) {
      iterationRestart = restartHighDefault;
      iterationStep = 1;
    } else if (iterationRestartField == null) {
      iterationStep = iterationStepField;
      if (iterationStep > 0) {
        iterationRestart = restartHighDefault;
      } else if (iterationStep < 0) {
        iterationRestart = -1 * restartLowDefault;
      } else {
        throw new RuntimeException(String.format(
            "%s field of %s property cannot be zero",
            ITERATION_PROP_STEP,
            ITERATION_PROP
        ));
      }
    } else if (iterationStepField == null) {
      iterationRestart = iterationRestartField;
      if (iterationRestart > iterationStart) {
        iterationStep = 1;
      } else if (iterationRestart < iterationStart) {
        iterationStep = -1;
      } else {
        throw new RuntimeException(String.format(
            "%s and %s fields of %s property cannot be equal",
            ITERATION_PROP_START,
            ITERATION_PROP_RESTART,
            ITERATION_PROP
        ));
      }
    } else {
      iterationRestart = iterationRestartField;
      iterationStep = iterationStepField;
      if (iterationStep == 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property cannot be zero",
            ITERATION_PROP_STEP,
            ITERATION_PROP
        ));
      }
      if (iterationStart == iterationRestart) {
        throw new RuntimeException(String.format(
            "%s and %s fields of %s property cannot be equal",
            ITERATION_PROP_START,
            ITERATION_PROP_RESTART,
            ITERATION_PROP
        ));
      }
      if (iterationRestart > iterationStart && iterationStep < 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property must be positive when %s field is greater than %s field",
            ITERATION_PROP_STEP,
            ITERATION_PROP,
            ITERATION_PROP_RESTART,
            ITERATION_PROP_START
        ));
      }
      if (iterationRestart < iterationStart && iterationStep > 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property must be negative when %s field is less than %s field",
            ITERATION_PROP_STEP,
            ITERATION_PROP,
            ITERATION_PROP_RESTART,
            ITERATION_PROP_START
        ));
      }
    }

    double iterationInitial = iterationStart;
    if (iterationInitialField != null) {
      iterationInitial = iterationInitialField;
    }

    return new DecimalIterator(
        iterationStart,
        iterationRestart,
        iterationStep,
        iterationInitial,
        generation,
        type
    );
  }

  private Iterator<Object> parseIterations(Schema schema, Map propertiesProp) {
    enforceMutualExclusion(
        propertiesProp, ITERATION_PROP,
        LENGTH_PROP, REGEX_PROP, OPTIONS_PROP, RANGE_PROP
    );

    Object iterationProp = propertiesProp.get(ITERATION_PROP);
    if (!(iterationProp instanceof Map)) {
      throw new RuntimeException(String.format(
          "%s prop must be an object, was %s instead",
          ITERATION_PROP,
          iterationProp.getClass().getName()
      ));
    }

    Map iterationProps = (Map) iterationProp;
    switch (schema.getType()) {
      case BOOLEAN:
        return getBooleanIterator(iterationProps);
      case INT:
        return getIntegerIterator(iterationProps);
      case LONG:
        return getLongIterator(iterationProps);
      case FLOAT:
        return getFloatIterator(iterationProps);
      case DOUBLE:
        return getDoubleIterator(iterationProps);
      case STRING:
        return createStringIterator(getIntegerIterator(iterationProps), propertiesProp);
      default:
        throw new UnsupportedOperationException(String.format(
            "%s property can only be specified on numeric, boolean or string schemas, "
            + "not %s schema",
            ITERATION_PROP,
            schema.getType().toString()
        ));
    }
  }

  private Iterator<Object> getDoubleIterator(final Map iterationProps) {
    Double iterationStartField = getDecimalNumberField(
        ITERATION_PROP,
        ITERATION_PROP_START,
        iterationProps
    );
    Double iterationRestartField = getDecimalNumberField(
        ITERATION_PROP,
        ITERATION_PROP_RESTART,
        iterationProps
    );
    Double iterationStepField = getDecimalNumberField(
        ITERATION_PROP,
        ITERATION_PROP_STEP,
        iterationProps
    );
    Double iterationInitialField = getDecimalNumberField(
        ITERATION_PROP,
        ITERATION_PROP_INITIAL,
        iterationProps
    );
    return getDecimalIterator(
        iterationStartField,
        iterationRestartField,
        iterationStepField,
        iterationInitialField,
        DecimalIterator.Type.DOUBLE
    );
  }

  private Iterator<Object> getFloatIterator(final Map iterationProps) {
    Float iterationStartField = getFloatNumberField(
        ITERATION_PROP,
        ITERATION_PROP_START,
        iterationProps
    );
    Float iterationRestartField = getFloatNumberField(
        ITERATION_PROP,
        ITERATION_PROP_RESTART,
        iterationProps
    );
    Float iterationStepField = getFloatNumberField(
        ITERATION_PROP,
        ITERATION_PROP_STEP,
        iterationProps
    );
    Float iterationInitialField = getFloatNumberField(
        ITERATION_PROP,
        ITERATION_PROP_INITIAL,
        iterationProps
    );
    return getDecimalIterator(
        iterationStartField != null ? iterationStartField.doubleValue() : null,
        iterationRestartField != null ? iterationRestartField.doubleValue() : null,
        iterationStepField != null ? iterationStepField.doubleValue() : null,
        iterationInitialField != null ? iterationInitialField.doubleValue() : null,
        DecimalIterator.Type.FLOAT
    );
  }

  private Iterator<Object> getLongIterator(final Map iterationProps) {
    Long iterationStartField = getIntegralNumberField(
        ITERATION_PROP,
        ITERATION_PROP_START,
        iterationProps
    );
    Long iterationRestartField = getIntegralNumberField(
        ITERATION_PROP,
        ITERATION_PROP_RESTART,
        iterationProps
    );
    Long iterationStepField = getIntegralNumberField(
        ITERATION_PROP,
        ITERATION_PROP_STEP,
        iterationProps
    );
    Long iterationInitialField = getIntegralNumberField(
        ITERATION_PROP,
        ITERATION_PROP_INITIAL,
        iterationProps
    );
    return getIntegralIterator(
        iterationStartField,
        iterationRestartField,
        iterationStepField,
        iterationInitialField,
        IntegralIterator.Type.LONG
    );
  }

  private Iterator<Object> createStringIterator(Iterator<Object> inner, Map propertiesProp) {
    return new Iterator<Object>() {
      @Override
      public boolean hasNext() {
        return inner.hasNext();
      }

      @Override
      public Object next() {
        return prefixAndSuffixString(inner.next().toString(), propertiesProp);
      }
    };
  }

  private Iterator<Object> getIntegerIterator(Map iterationProps) {
    Integer iterationStartField = getIntegerNumberField(
        ITERATION_PROP,
        ITERATION_PROP_START,
        iterationProps
    );
    Integer iterationRestartField = getIntegerNumberField(
        ITERATION_PROP,
        ITERATION_PROP_RESTART,
        iterationProps
    );
    Integer iterationStepField = getIntegerNumberField(
        ITERATION_PROP,
        ITERATION_PROP_STEP,
        iterationProps
    );
    Integer iterationInitialField = getIntegerNumberField(
        ITERATION_PROP,
        ITERATION_PROP_INITIAL,
        iterationProps
    );
    return getIntegralIterator(
        iterationStartField != null ? iterationStartField.longValue() : null,
        iterationRestartField != null ? iterationRestartField.longValue() : null,
        iterationStepField != null ? iterationStepField.longValue() : null,
        iterationInitialField != null ? iterationInitialField.longValue() : null,
        IntegralIterator.Type.INTEGER
    );
  }

  @SuppressWarnings("unchecked")
  private <T> T generateIteration(Schema schema, Map propertiesProp) {
    if (!iteratorCache.containsKey(schema)) {
      iteratorCache.put(schema, parseIterations(schema, propertiesProp));
    }
    return (T) iteratorCache.get(schema).next();
  }

  private Collection<Object> generateArray(Schema schema, Map propertiesProp) {
    int length = getLengthBounds(propertiesProp).random();
    Collection<Object> result = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      result.add(generateObject(schema.getElementType()));
    }
    return result;
  }

  private Boolean generateBoolean(Map propertiesProp) {
    Double odds = getDecimalNumberField(ARG_PROPERTIES_PROP, ODDS_PROP, propertiesProp);
    if (odds == null) {
      return random.nextBoolean();
    } else {
      if (odds < 0.0 || odds > 1.0) {
        throw new RuntimeException(String.format(
            "%s property must be in the range [0.0, 1.0]",
            ODDS_PROP
        ));
      }
      return random.nextDouble() < odds;
    }
  }

  private ByteBuffer generateBytes(Schema schema, Map propertiesProp) {
    LogicalTypes.Decimal decimalLogicalType = getDecimalLogicalType(schema);
    byte[] bytes;
    if (decimalLogicalType != null) {
      bytes = generateDecimal(decimalLogicalType, propertiesProp);
    } else {
      bytes = new byte[getLengthBounds(propertiesProp.get(LENGTH_PROP)).random()];
      random.nextBytes(bytes);
    }
    return ByteBuffer.wrap(bytes);
  }

  private Double generateDouble(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Double rangeMinField = getDecimalNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Double rangeMaxField = getDecimalNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        double rangeMin = rangeMinField != null ? rangeMinField : -1 * Double.MAX_VALUE;
        double rangeMax = rangeMaxField != null ? rangeMaxField : Double.MAX_VALUE;
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + (random.nextDouble() * (rangeMax - rangeMin));
      } else {
        throw new RuntimeException(String.format(
            "%s property must be an object",
            RANGE_PROP
        ));
      }
    }
    return random.nextDouble();
  }

  private GenericEnumSymbol generateEnumSymbol(Schema schema) {
    List<String> enums = schema.getEnumSymbols();
    return new
        GenericData.EnumSymbol(schema, enums.get(random.nextInt(enums.size())));
  }

  private GenericFixed generateFixed(Schema schema) {
    LogicalTypes.Decimal decimalLogicalType = getDecimalLogicalType(schema);
    byte[] bytes;
    if (decimalLogicalType != null) {
      // We don't support ranges for fixed decimal types at the moment
      bytes = generateDecimal(decimalLogicalType, null);
    } else {
      bytes = new byte[schema.getFixedSize()];
      random.nextBytes(bytes);
    }
    return new GenericData.Fixed(schema, bytes);
  }

  private byte[] generateDecimal(LogicalTypes.Decimal decimalLogicalType, Map propertiesProp) {
    Object rangeProp = Optional.ofNullable(propertiesProp).map(m -> m.get(RANGE_PROP)).orElse(null);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Double rangeMinField = getDecimalNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Double rangeMaxField = getDecimalNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        double rangeMin = rangeMinField != null ? rangeMinField : -1 * Math.pow(10, decimalLogicalType.getPrecision()
                - decimalLogicalType.getScale());
        double rangeMax = rangeMaxField != null ? rangeMaxField : Math.pow(10, decimalLogicalType.getPrecision()
                - decimalLogicalType.getScale());
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        // We'll just generate a random double in the requested range and then convert it to a logical decimal type
        double result = rangeMin + (random.nextDouble() * (rangeMax - rangeMin));
        return BigDecimal.valueOf(result)
            // Adjust by the scale of the decimal type in order to get the "unscaled" value described below before
            // converting to a twos-complement byte array
            .scaleByPowerOfTen(decimalLogicalType.getScale())
            .toBigInteger()
            .toByteArray();
      } else {
        throw new RuntimeException(String.format(
            "%s property must be an object",
            RANGE_PROP
        ));
      }
    } else {
        /*
          According to the Avro 1.9.1 spec (http://avro.apache.org/docs/1.9.1/spec.html#Decimal):

          "The decimal logical type represents an arbitrary-precision signed decimal number of the form
        unscaled × 10-scale.

          "A decimal logical type annotates Avro bytes or fixed types. The byte array must contain the
        two's-complement representation of the unscaled integer value in big-endian byte order. The scale
        is fixed, and is specified using an attribute."

          We generate a random decimal here by starting with a value of zero, then repeatedly multiplying
        by 10^15 (15 is the minimum number of significant digits in a double), and adding a new random
        value in the range [0, 10^15) generated using the Random object for this generator. This is done
        until the precision of the current value is equal to or greater than the precision of the logical
        type. At this point, any extra digits (of there should be at most 14) are rounded off from the
        value, a sign is randomly selected, it is converted to big-endian two's-complement representation,
        and returned.
         */
      BigInteger bigInteger = BigInteger.ZERO;
      final long maxIncrementExclusive = 1_000_000_000_000_000L;
      int precision;
      for (precision = 0; precision < decimalLogicalType.getPrecision(); precision += 15) {
        bigInteger = bigInteger.multiply(BigInteger.valueOf(maxIncrementExclusive));
        long increment = (long) (random.nextDouble() * maxIncrementExclusive);
        bigInteger = bigInteger.add(BigInteger.valueOf(increment));
      }
      bigInteger = bigInteger.divide(
          BigInteger.TEN.pow(precision - decimalLogicalType.getPrecision())
      );
      if (random.nextBoolean()) {
        bigInteger = bigInteger.negate();
      }
      return bigInteger.toByteArray();
    }
  }

  private Float generateFloat(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Float rangeMinField = getFloatNumberField(
            RANGE_PROP,
            RANGE_PROP_MIN,
            rangeProps
        );
        Float rangeMaxField = getFloatNumberField(
            RANGE_PROP,
            RANGE_PROP_MAX,
            rangeProps
        );
        float rangeMin = Optional.ofNullable(rangeMinField).orElse(-1 * Float.MAX_VALUE);
        float rangeMax = Optional.ofNullable(rangeMaxField).orElse(Float.MAX_VALUE);
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + (random.nextFloat() * (rangeMax - rangeMin));
      }
    }
    return random.nextFloat();
  }

  private Integer generateInt(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Integer rangeMinField = getIntegerNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Integer rangeMaxField = getIntegerNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        int rangeMin = Optional.ofNullable(rangeMinField).orElse(Integer.MIN_VALUE);
        int rangeMax = Optional.ofNullable(rangeMaxField).orElse(Integer.MAX_VALUE);
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + ((int) (random.nextDouble() * (rangeMax - rangeMin)));
      }
    }
    return random.nextInt();
  }

  private Long generateLong(Map propertiesProp) {
    Object rangeProp = propertiesProp.get(RANGE_PROP);
    if (rangeProp != null) {
      if (rangeProp instanceof Map) {
        Map rangeProps = (Map) rangeProp;
        Long rangeMinField = getIntegralNumberField(RANGE_PROP, RANGE_PROP_MIN, rangeProps);
        Long rangeMaxField = getIntegralNumberField(RANGE_PROP, RANGE_PROP_MAX, rangeProps);
        long rangeMin = Optional.ofNullable(rangeMinField).orElse(Long.MIN_VALUE);
        long rangeMax = Optional.ofNullable(rangeMaxField).orElse(Long.MAX_VALUE);
        if (rangeMin >= rangeMax) {
          throw new RuntimeException(String.format(
              "'%s' field must be strictly less than '%s' field in %s property",
              RANGE_PROP_MIN,
              RANGE_PROP_MAX,
              RANGE_PROP
          ));
        }
        return rangeMin + (((long) (random.nextDouble() * (rangeMax - rangeMin))));
      }
    }
    return random.nextLong();
  }

  private Map<String, Object> generateMap(Schema schema, Map propertiesProp) {
    Map<String, Object> result = new HashMap<>();
    int length = getLengthBounds(propertiesProp).random();
    Object keyProp = propertiesProp.get(KEYS_PROP);
    if (keyProp == null) {
      for (int i = 0; i < length; i++) {
        result.put(generateRandomString(1), generateObject(schema.getValueType()));
      }
    } else if (keyProp instanceof Map) {
      Map keyPropMap = (Map) keyProp;
      if (keyPropMap.containsKey(OPTIONS_PROP)) {
        if (!optionsCache.containsKey(schema)) {
          optionsCache.put(schema, parseOptions(Schema.create(Schema.Type.STRING), keyPropMap));
        }
        for (int i = 0; i < length; i++) {
          result.put(generateOption(schema, keyPropMap), generateObject(schema.getValueType()));
        }
      } else {
        for (int i = 0; i < length; i++) {
          result.put(
              generateString(schema, keyPropMap),
              generateObject(schema.getValueType())
          );
        }
      }
    } else {
      throw new RuntimeException(String.format(
          "%s prop must be an object",
          KEYS_PROP
      ));
    }
    return result;
  }

  private Object generateNull() {
    return null;
  }

  private GenericRecord generateRecord(Schema schema) {
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field, generateObject(field.schema()));
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private String generateRegexString(Schema schema, Object regexProp, LengthBounds lengthBounds) {
    if (!generexCache.containsKey(schema)) {
      if (!(regexProp instanceof String)) {
        throw new RuntimeException(String.format("%s property must be a string", REGEX_PROP));
      }
      generexCache.put(schema, new Generex((String) regexProp, random));
    }
    // Generex.random(low, high) generates in range [low, high]; we want [low, high), so subtract
    // 1 from maxLength
    return generexCache.get(schema).random(lengthBounds.min(), lengthBounds.max() - 1);
  }

  private String generateRandomString(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) random.nextInt(128);
    }
    return new String(bytes, StandardCharsets.US_ASCII);
  }

  private String generateString(Schema schema, Map propertiesProp) {
    Object regexProp = propertiesProp.get(REGEX_PROP);

    String result;
    if (regexProp != null) {
      Object lengthProp = propertiesProp.get(LENGTH_PROP);
      LengthBounds lengthBounds;
      if (lengthProp == null) {
        lengthBounds = new LengthBounds(0, Integer.MAX_VALUE);
      } else {
        lengthBounds = getLengthBounds(lengthProp);
      }
      result = generateRegexString(schema, regexProp, lengthBounds);
    } else {
      result = generateRandomString(getLengthBounds(propertiesProp).random());
    }

    return  prefixAndSuffixString(result, propertiesProp);
  }

  private String prefixAndSuffixString(String result, Map propertiesProp) {
    Object prefixProp = propertiesProp.get(PREFIX_PROP);
    if (prefixProp != null && !(prefixProp instanceof String)) {
      throw new RuntimeException(String.format("%s property must be a string", PREFIX_PROP));
    }
    String prefix = prefixProp != null ? (String) prefixProp : "";

    Object suffixProp = propertiesProp.get(SUFFIX_PROP);
    if (suffixProp != null && !(suffixProp instanceof String)) {
      throw new RuntimeException(String.format("%s property must be a string", SUFFIX_PROP));
    }
    String suffix = suffixProp != null ? (String) suffixProp : "";

    return prefix + result + suffix;
  }

  private Object generateUnion(Schema schema) {
    List<Schema> schemas = schema.getTypes();
    return generateObject(schemas.get(random.nextInt(schemas.size())));
  }

  private LengthBounds getLengthBounds(Map propertiesProp) {
    return getLengthBounds(propertiesProp.get(LENGTH_PROP));
  }

  private LengthBounds getLengthBounds(Object lengthProp) {
    if (lengthProp == null) {
      return new LengthBounds();
    } else if (lengthProp instanceof Integer) {
      Integer length = (Integer) lengthProp;
      if (length < 0) {
        throw new RuntimeException(String.format(
            "when given as integral number, %s property cannot be negative",
            LENGTH_PROP
        ));
      }
      return new LengthBounds(length);
    } else if (lengthProp instanceof Map) {
      Map lengthProps = (Map) lengthProp;
      Integer minLength = getIntegerNumberField(LENGTH_PROP, LENGTH_PROP_MIN, lengthProps);
      Integer maxLength = getIntegerNumberField(LENGTH_PROP, LENGTH_PROP_MAX, lengthProps);
      if (minLength == null && maxLength == null) {
        throw new RuntimeException(String.format(
            "%s property must contain at least one of '%s' or '%s' fields when given as object",
            LENGTH_PROP,
            LENGTH_PROP_MIN,
            LENGTH_PROP_MAX
        ));
      }
      minLength = minLength != null ? minLength : 0;
      maxLength = maxLength != null ? maxLength : Integer.MAX_VALUE;
      if (minLength < 0) {
        throw new RuntimeException(String.format(
            "%s field of %s property cannot be negative",
            LENGTH_PROP_MIN,
            LENGTH_PROP
        ));
      }
      if (maxLength <= minLength) {
        throw new RuntimeException(String.format(
            "%s field must be strictly greater than %s field for %s property",
            LENGTH_PROP_MAX,
            LENGTH_PROP_MIN,
            LENGTH_PROP
        ));
      }
      return new LengthBounds(minLength, maxLength);
    } else {
      throw new RuntimeException(String.format(
          "%s property must either be an integral number or an object, was %s instead",
          LENGTH_PROP,
          lengthProp.getClass().getName()
      ));
    }
  }

  private Integer getIntegerNumberField(String property, String field, Map propsMap) {
    Long result = getIntegralNumberField(property, field, propsMap);
    if (result != null && (result < Integer.MIN_VALUE || result > Integer.MAX_VALUE)) {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be a valid int for int schemas",
          field,
          property
      ));
    }
    return result != null ? result.intValue() : null;
  }

  private Long getIntegralNumberField(String property, String field, Map propsMap) {
    Object result = propsMap.get(field);
    if (result == null || result instanceof Long) {
      return (Long) result;
    } else if (result instanceof Integer) {
      return ((Integer) result).longValue();
    } else {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be an integral number, was %s instead",
          field,
          property,
          result.getClass().getName()
      ));
    }
  }

  private Float getFloatNumberField(String property, String field, Map propsMap) {
    Double result = getDecimalNumberField(property, field, propsMap);
    if (result != null && (result > Float.MAX_VALUE || result < -1 * Float.MAX_VALUE)) {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be a valid float for float schemas",
          field,
          property
      ));
    }
    return result != null ? result.floatValue() : null;
  }

  private Double getDecimalNumberField(String property, String field, Map propsMap) {
    Object result = propsMap.get(field);
    if (result == null || result instanceof Double) {
      return (Double) result;
    } else if (result instanceof Float) {
      return ((Float) result).doubleValue();
    } else if (result instanceof Integer) {
      return ((Integer) result).doubleValue();
    } else if (result instanceof Long) {
      return ((Long) result).doubleValue();
    } else {
      throw new RuntimeException(String.format(
          "'%s' field of %s property must be a number, was %s instead",
          field,
          property,
          result.getClass().getName()
      ));
    }
  }

  private class LengthBounds {
    public static final int DEFAULT_MIN = 8;
    public static final int DEFAULT_MAX = 16;

    private final int min;
    private final int max;

    LengthBounds(int min, int max) {
      this.min = min;
      this.max = max;
    }

    LengthBounds(int exact) {
      this(exact, exact + 1);
    }

    LengthBounds() {
      this(DEFAULT_MIN, DEFAULT_MAX);
    }

    public int random() {
      return min + random.nextInt(max - min);
    }

    public int min() {
      return min;
    }

    public int max() {
      return max;
    }
  }

  private static class IntegralIterator implements Iterator<Object> {
    public enum Type {
      INTEGER, LONG
    }

    private final BigInteger start;
    private final BigInteger restart;
    private final BigInteger step;
    private final Type type;
    private BigInteger current;

    IntegralIterator(long start, long restart, long step, long initial, long count, Type type) {
      this.start = BigInteger.valueOf(start);
      this.restart = BigInteger.valueOf(restart);
      this.step = BigInteger.valueOf(step);
      this.type = type;
      current = BigInteger.valueOf(initial).subtract(this.start);
      if (count > 0) {
        // This is essentially the following expression when ignoring negative values:
        // current = (count * step) % (restart - start)
        // except BigInteger::mod only operates on positive numbers, so remove and re-add the sign after the modulo.
        current = BigInteger.valueOf(count)
            .multiply(this.step)
            .add(current)
            .abs()
            .mod(this.restart.subtract(this.start).abs())
            .multiply(this.step.divide(this.step.abs()));
      }
    }

    @Override
    public Object next() {
      BigInteger result = current.add(start);
      current = current
          .add(step)
          .abs()
          .mod(restart.subtract(start).abs())
          .multiply(step.divide(step.abs()));

      switch (type) {
        case INTEGER:
          return result.intValue();
        case LONG:
          return result.longValue();
        default:
          throw new RuntimeException(String.format("Unexpected Type: %s", type));
      }
    }

    @Override
    public boolean hasNext() {
      return true;
    }
  }

  private static class DecimalIterator implements Iterator<Object> {
    public enum Type {
      FLOAT, DOUBLE
    }

    private final BigDecimal start;
    private final BigDecimal restart;
    private final BigDecimal modulo;
    private final BigDecimal step;
    private final Type type;
    private BigDecimal current;

    DecimalIterator(double start, double restart, double step, double initial, long count, Type type) {
      this.start = BigDecimal.valueOf(start);
      this.restart = BigDecimal.valueOf(restart);
      this.modulo = this.restart.subtract(this.start);
      this.step = BigDecimal.valueOf(step);
      this.type = type;
      current = BigDecimal.valueOf(initial).subtract(this.start);
      if (count > 0) {
        current = BigDecimal.valueOf(count)
            .multiply(this.step)
            .add(current)
            .remainder(this.modulo);
      }
    }

    @Override
    public Object next() {
      BigDecimal result = current.add(start);
      current = current
          .add(step)
          .remainder(modulo);
      switch (type) {
        case FLOAT:
          return result.floatValue();
        case DOUBLE:
          return result.doubleValue();
        default:
          throw new RuntimeException(String.format("Unexpected Type: %s", type));
      }
    }

    @Override
    public boolean hasNext() {
      return true;
    }
  }

  private static class BooleanIterator implements Iterator<Object> {
    private boolean current;

    BooleanIterator(boolean start) {
      current = start;
    }

    @Override
    public Boolean next() {
      boolean result = current;
      current = !current;
      return result;
    }

    @Override
    public boolean hasNext() {
      return true;
    }
  }
}
