/*
 * Copyright 2024 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.configservice.service.config;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.grayReleaseRule.GrayReleaseRulesHolder;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.ReleaseMessageService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

/**
 * @author jason
 */
@RunWith(MockitoJUnitRunner.class)
public class ConfigServiceWithChangeCacheTest {
  private ConfigServiceWithChangeCache configServiceWithChangeCache;

  @Mock
  private ReleaseService releaseService;
  @Mock
  private ReleaseMessageService releaseMessageService;

  @Mock
  private BizConfig bizConfig;
  @Mock
  private MeterRegistry meterRegistry;
  @Mock
  private GrayReleaseRulesHolder grayReleaseRulesHolder;

  @Before
  public void setUp() throws Exception {
    configServiceWithChangeCache = new ConfigServiceWithChangeCache(releaseService, releaseMessageService,
                                                              grayReleaseRulesHolder, bizConfig, meterRegistry);

    configServiceWithChangeCache.initialize();
  }
  @Test
  public void testChangeConfigurationsWithAdd() {
    String key1 = "key1";
    String value1 = "value1";

    String key2 = "key2";
    String value2 = "value2";

    Map<String, String> latestConfig = ImmutableMap.of(key1, value1,key2, value2);
    Map<String, String> historyConfig = ImmutableMap.of(key1, value1);

    Map<String, String> result =
            configServiceWithChangeCache.changeConfigurations(latestConfig,historyConfig);

    assertEquals(1, result.keySet().size());
    assertEquals(value2, result.get(key2));
  }
  @Test
  public void testChangeConfigurationsWithUpdate() {
    String key1 = "key1";
    String value1 = "value1";

    String anotherValue1 = "anotherValue1";

    Map<String, String> latestConfig = ImmutableMap.of(key1, value1);
    Map<String, String> historyConfig = ImmutableMap.of(key1, anotherValue1);

    Map<String, String> result =
            configServiceWithChangeCache.changeConfigurations(latestConfig,historyConfig);

    assertEquals(1, result.keySet().size());
    assertEquals(value1, result.get(key1));
  }
  @Test
  public void testChangeConfigurationsWithDelete() {
    String key1 = "key1";
    String value1 = "value1";

    Map<String, String> latestConfig = ImmutableMap.of();
    Map<String, String> historyConfig = ImmutableMap.of(key1, value1);

    Map<String, String> result =
            configServiceWithChangeCache.changeConfigurations(latestConfig,historyConfig);

    assertEquals(1, result.keySet().size());
    assertEquals("", result.get(key1));
  }

}
