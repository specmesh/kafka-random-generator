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

package io.specmesh.avro.random.generator.util;

import io.specmesh.avro.random.generator.GeneratorTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;


public final class ResourceUtil {

  private ResourceUtil() {
  }

  /**
   * Load file contents from classpath as string.
   * @param filePath the relative resource path.
   * @return the file contents.
   */
  public static String loadContent(final String filePath) {
    final InputStream testDir =
        GeneratorTest.class.getClassLoader().getResourceAsStream(filePath);

    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(testDir, StandardCharsets.UTF_8))) {

      return reader.lines().collect(Collectors.joining("\n"));
    } catch (final IOException ioe) {
      throw new RuntimeException("failed to find test test-schema " + filePath, ioe);
    }
  }
}
