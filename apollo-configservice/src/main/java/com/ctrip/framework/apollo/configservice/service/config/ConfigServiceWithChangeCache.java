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
import com.ctrip.framework.apollo.common.entity.KVEntity;
import com.ctrip.framework.apollo.configservice.dto.ReleaseCompareResultDTO;
import com.ctrip.framework.apollo.configservice.enums.ChangeType;
import com.ctrip.framework.apollo.core.dto.ConfigurationChange;
import com.ctrip.framework.apollo.core.enums.ConfigurationChangeType;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * config service with change cache
 *
 * @author Jason
 */
public class ConfigServiceWithChangeCache extends ConfigServiceWithCache{
  private static final Logger logger = LoggerFactory.getLogger(ConfigServiceWithChangeCache.class);


  private static final long DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS = 10;


  public LoadingCache<String,  Optional<Release>> releaseKeyCache;


  public ConfigServiceWithChangeCache(final ReleaseService releaseService,
                                final ReleaseMessageService releaseMessageService,
                                final GrayReleaseRulesHolder grayReleaseRulesHolder,
                                final BizConfig bizConfig,
                                final MeterRegistry meterRegistry){
    super(releaseService, releaseMessageService, grayReleaseRulesHolder, bizConfig, meterRegistry);

  }

  @PostConstruct
  public void initialize() {
    buildReleaseKeyCache();
  }

  private void buildReleaseKeyCache() {
    CacheBuilder mergedReleaseCacheBuilder = CacheBuilder.newBuilder()
            .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS, TimeUnit.SECONDS);

    releaseKeyCache = mergedReleaseCacheBuilder.build(new CacheLoader<String, Optional<Release>>() {
      @Override
      public  Optional<Release> load(String key) throws Exception {
        Release release = releaseService.findByReleaseKey(key);
        return Optional.ofNullable(release);
      }
    });
  }

  public List<ConfigurationChange> calcConfigurationChanges(Map<String, String> latestReleaseConfigurations, Map<String, String> historyConfigurations){
    if (latestReleaseConfigurations == null) {
      latestReleaseConfigurations = new HashMap<>();
    }

    if (historyConfigurations == null) {
      historyConfigurations =  new HashMap<>();
    }

    Set<String> previousKeys = historyConfigurations.keySet();
    Set<String> currentKeys = latestReleaseConfigurations.keySet();

    Set<String> commonKeys = Sets.intersection(previousKeys, currentKeys);
    Set<String> newKeys = Sets.difference(currentKeys, commonKeys);
    Set<String> removedKeys = Sets.difference(previousKeys, commonKeys);

    List<ConfigurationChange> changes = Lists.newArrayList();

    for (String newKey : newKeys) {
      changes.add(new ConfigurationChange( newKey,latestReleaseConfigurations.get(newKey), ConfigurationChangeType.ADDED));
    }

    for (String removedKey : removedKeys) {
      changes.add(new ConfigurationChange( removedKey, null, ConfigurationChangeType.DELETED));
    }

    for (String commonKey : commonKeys) {
      String previousValue = historyConfigurations.get(commonKey);
      String currentValue = latestReleaseConfigurations.get(commonKey);
      if (Objects.equal(previousValue, currentValue)) {
        continue;
      }
      changes.add(new ConfigurationChange(commonKey,currentValue,ConfigurationChangeType.MODIFIED));
    }

    return changes;
  }


  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    logger.info("message received - channel: {}, message: {}", channel, message);
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(message.getMessage())) {
      return;
    }

    try {
      String messageKey = message.getMessage();
      if (bizConfig.isConfigServiceCacheKeyIgnoreCase()) {
        messageKey = messageKey.toLowerCase();
      }
      List<String> namespaceInfo = ReleaseMessageKeyGenerator.messageToList(messageKey);
      Release latestRelease = releaseService.findLatestActiveRelease(namespaceInfo.get(0), namespaceInfo.get(1),
                                                                     namespaceInfo.get(2));
      releaseKeyCache.put(latestRelease.getReleaseKey(), Optional.ofNullable(latestRelease));
    } catch (Throwable ex) {
      //ignore
    }
  }

  @Override
  public  Map<String,Release> findReleasesByReleaseKeys(Set<String> releaseKeys){
    //只要有一个拿不到，就返回null
    try {
      //从缓存拿到配置
      ImmutableMap<String, Optional<Release>> releaseKeysMap= releaseKeyCache.getAll(releaseKeys);
      //过滤value 为null的值
      Map<String,Release> filterReleaseKeysMap= releaseKeysMap.entrySet().stream()
              .filter(entry->entry.getValue().isPresent()).collect(Collectors.toMap(Map.Entry::getKey, entry->entry.getValue().get()));
      if(releaseKeys.size()==filterReleaseKeysMap.size()){
        //历史缓存找不到 1、过期 2、增量开关
        return filterReleaseKeysMap;
      }
    } catch (ExecutionException e) {
      //如果value 为null，就返回异常
    }
    return null;
  }
}
