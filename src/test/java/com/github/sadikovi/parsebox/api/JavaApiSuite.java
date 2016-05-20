/*
 * Copyright 2016 sadikovi
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

package com.github.sadikovi.parsebox.api;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/** Testing of Java API for Parsebox */
public class JavaApiSuite {

  @Test
  public void testExternalParser() {
    ExternalParser ep = new ExternalParser(Opt2RecordType.class) {
      @Override
      public DataFrame create() {
        return null;
      }
    };
    assertEquals(ep.dataSchema(), new Opt2RecordType().dataSchema());
  }

  @Test
  public void testBaseFormat() {
    BaseFormat bf = new BaseFormat() {
      @Override
      public ResolvedParser createParser(
          SQLContext sqlContext,
          String[] paths,
          Map<String, String> parameters) {
        return null;
      }
    };

    assertEquals(bf.createParser(null, null, new HashMap<String, String>()), null);
  }
}
