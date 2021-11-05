/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package charley.wu.flink.datahub.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

@Slf4j
public final class ConfigUtil {

  private static final String CONFIG_FILE = "configFile";
  private static final String CONFIG_PROP = "/config.properties";

  public static int getInteger(Properties props, String key, int defaultValue) {
    return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
  }

  public static long getLong(Properties props, String key, long defaultValue) {
    return Long.parseLong(props.getProperty(key, String.valueOf(defaultValue)));
  }

  public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
    return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
  }

  public static Properties getCustomParams(ParameterTool args) throws IOException {
    ParameterTool tool = ParameterTool.fromPropertiesFile(args.get(CONFIG_FILE));
    return tool.getProperties();
  }

  public static Properties getPropertiesParams() throws IOException {
    InputStream inputStream = ConfigUtil.class.getResourceAsStream(CONFIG_PROP);
    if (inputStream == null) {
      log.info("Load config file error, can not get File, {}", CONFIG_PROP);
      throw new IllegalArgumentException(
          "Load config file error, can not get File, file:" + CONFIG_PROP);
    }
    ParameterTool tool = ParameterTool.fromPropertiesFile(inputStream);
    return tool.getProperties();
  }

  public static Properties getWithPrefix(String prefix, Properties props) {
    Properties newProps = new Properties();
    for (Object keyObj : props.keySet()) {
      String key = (String) keyObj;
      if (key.startsWith(prefix)) {
        String newKey = key.substring(prefix.length());
        newProps.setProperty(newKey, props.getProperty(key));
      }
    }
    return newProps;
  }
}
