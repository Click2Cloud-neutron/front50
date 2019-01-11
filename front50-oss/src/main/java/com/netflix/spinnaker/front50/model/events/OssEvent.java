/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.front50.model.events;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class OssEvent {
  @JsonProperty("Records")
  public List<OssEventRecord> records;

  public static class OssEventRecord {
    public String eventName;
    public String eventTime;
    public OssMeta oss;
  }

  public static class OssMeta {
    public OssObject object;
  }

  public static class OssObject {
    public String key;
  }
}
