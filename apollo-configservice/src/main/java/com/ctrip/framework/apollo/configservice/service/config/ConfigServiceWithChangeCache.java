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
import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.entity.KVEntity;
import com.ctrip.framework.apollo.configservice.dto.ReleaseCompareResultDTO;
import com.ctrip.framework.apollo.configservice.enums.ChangeType;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * config service with change cache
 *
 * @author Jason
 */
public class ConfigServiceWithChangeCache extends ConfigServiceWithCache implements IncrementalSyncConfigService {
  private static final Logger logger = LoggerFactory.getLogger(ConfigServiceWithChangeCache.class);


  private static final Gson GSON = new Gson();

  private static final long DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS = 10;

  private LoadingCache<String, ConfigChangeCacheEntry> configChangeCache;

  private ConfigHistories configHistories;


  public ConfigServiceWithChangeCache(final ReleaseService releaseService,
                                      final ReleaseMessageService releaseMessageService,
                                      final GrayReleaseRulesHolder grayReleaseRulesHolder,
                                      final BizConfig bizConfig,
                                      final MeterRegistry meterRegistry) {
    super(releaseService,releaseMessageService,grayReleaseRulesHolder,bizConfig,meterRegistry);
    configHistories = new ConfigHistories();
  }

  @PostConstruct
  void initialize() {
    buildConfigChangeCache();
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
      updateConfigChangeCacheManager(messageKey);
    } catch (Throwable ex) {
      //ignore
    }
  }
  private void updateConfigChangeCacheManager(String messageKey){
    configCache.invalidate(messageKey);
    configCache.getUnchecked(messageKey);

    configHistories.addOne(messageKey);

    configChangeCache.invalidate(messageKey);
    configChangeCache.getUnchecked(messageKey);
  }
  @Override
  public Map<String,String> findLatestActiveChangeConfigurations(String appId, String clusterName, String namespaceName,
                                                                 ApolloNotificationMessages clientMessages, long historyNotificationId) {
    String messageKey = ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName);
    String cacheKey = messageKey;

    if (bizConfig.isConfigServiceCacheKeyIgnoreCase()) {
      cacheKey = cacheKey.toLowerCase();
    }

    ConfigChangeCacheEntry configChangeCacheEntry = configChangeCache.getUnchecked(cacheKey);

    //cache is out-dated
    if (clientMessages != null && clientMessages.has(messageKey) &&
            clientMessages.get(messageKey) > configChangeCacheEntry.getNotificationId()) {
      //invalidate the cache and try to load from db again
      updateConfigChangeCacheManager(messageKey);
      configChangeCacheEntry = configChangeCache.getUnchecked(cacheKey);
    }

    return configChangeCacheEntry.getConfigurationsByNotificationId(historyNotificationId);
  }

  private void buildConfigChangeCache() {
    CacheBuilder configChangeCacheBuilder = CacheBuilder.newBuilder()
            .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS, TimeUnit.SECONDS);
    if (bizConfig.isConfigServiceCacheStatsEnabled()) {
      configChangeCacheBuilder.recordStats();
    }

    configChangeCache = configChangeCacheBuilder.build(new CacheLoader<String, ConfigChangeCacheEntry>() {
      @Override
      public ConfigChangeCacheEntry load(String key) throws Exception {
        List<String> namespaceInfo = ReleaseMessageKeyGenerator.messageToList(key);
        if (CollectionUtils.isEmpty(namespaceInfo)) {
          Tracer.logError(
                  new IllegalArgumentException(String.format("Invalid cache load key %s", key)));
          return new ConfigChangeCacheEntry(null, null);
        }

        try {
          ConfigCacheEntry configCacheEntry=configCache.getUnchecked(key);
          Release latestRelease=configCacheEntry.getRelease();
          long notificationId=configCacheEntry.getNotificationId();

          ConfigChangeCacheEntry configChangeCacheEntry=new ConfigChangeCacheEntry(latestRelease,notificationId);


          LoadingCache<Long, Release> releaseHistories = configHistories.getReleaseHistories(key);

          for(Map.Entry<Long,Release> entry:releaseHistories.asMap().entrySet()){
            Release historiestrelease=entry.getValue();
            configChangeCacheEntry.addOne(historiestrelease);
          }



          return configChangeCacheEntry;
        } catch (Throwable ex) {

          throw ex;
        } finally {

        }
      }
    });

  }

  //base是旧的，toCompare是新的
  public ReleaseCompareResultDTO compare(Release baseRelease, Release toCompareRelease) {
    Map<String, String> baseReleaseConfiguration = baseRelease == null ? new HashMap<>() :
            GSON.fromJson(baseRelease.getConfigurations(), GsonType.CONFIG);
    Map<String, String> toCompareReleaseConfiguration = toCompareRelease == null ? new HashMap<>() :
            GSON.fromJson(toCompareRelease.getConfigurations(),
                          GsonType.CONFIG);

    ReleaseCompareResultDTO compareResult = new ReleaseCompareResultDTO();

    //added and modified in firstRelease
    for (Map.Entry<String, String> entry : baseReleaseConfiguration.entrySet()) {
      String key = entry.getKey();
      String firstValue = entry.getValue();
      String secondValue = toCompareReleaseConfiguration.get(key);
      //added
      if (secondValue == null) {
        compareResult.addEntityPair(ChangeType.DELETED, new KVEntity(key, firstValue),
                                    new KVEntity(key, null));
      } else if (!Objects.equal(firstValue, secondValue)) {
        compareResult.addEntityPair(ChangeType.MODIFIED, new KVEntity(key, firstValue),
                                    new KVEntity(key, secondValue));
      }

    }

    //deleted in firstRelease
    for (Map.Entry<String, String> entry : toCompareReleaseConfiguration.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (baseReleaseConfiguration.get(key) == null) {
        compareResult
                .addEntityPair(ChangeType.ADDED, new KVEntity(key, ""), new KVEntity(key, value));
      }

    }

    return compareResult;
  }

  private  class ConfigChangeCacheEntry {
    private final Map<String,ReleaseCompareResultDTO> releaseChanges;

    private final Release latestRelease;

    private final Long notificationId;
    public ConfigChangeCacheEntry(Release latestRelease,Long notificationId) {
      this.releaseChanges = new HashMap<>();
      this.latestRelease=latestRelease;
      this.notificationId=notificationId;
    }

    public Long  getNotificationId() {
      return notificationId;
    }


    public Map<String,ReleaseCompareResultDTO> getReleaseChanges() {
      return releaseChanges;
    }
    public Release getLatestRelease() {
      return latestRelease;
    }


    private void addOne(Release historyRelease){
      ReleaseCompareResultDTO releaseCompareResultDTO;
      releaseCompareResultDTO=compare(latestRelease,historyRelease);

      releaseChanges.put(getChangeKey(historyRelease.getId()),releaseCompareResultDTO);
    }

    private Map<String,String> getConfigurationsByNotificationId(long historyNotificationId){
      String key=getChangeKey(historyNotificationId);
      if(releaseChanges.get(key)==null){
        //不会存在两个id一样，导致为空，上一层已经判断过了。
        //所以这里的空只有没有命中缓存
        return new HashMap<>();
      }
      ReleaseCompareResultDTO releaseCompareResultDTO=releaseChanges.get(getChangeKey(historyNotificationId));
      return releaseCompareResultDTO.getFirstEntitys().stream().collect(Collectors.toMap(KVEntity::getKey, KVEntity::getValue));
    }

    private String getChangeKey(long historiesReleaseId){
      return latestRelease.getId()+"+"+historiesReleaseId;
    }

  }

  private class ConfigHistoriesCacheEntry {
    //[releasemessageId,release] 用于存放历史的releaseMap
    private LoadingCache<Long, Release> releaseHistories;


    public ConfigHistoriesCacheEntry(long maximumSize) {
      releaseHistories = CacheBuilder.newBuilder()
              .maximumSize(maximumSize)
              .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS, TimeUnit.SECONDS).build(new CacheLoader<Long, Release>() {
                @Override
                public Release load(Long key) throws Exception {
                  return null;
                }
              });
    }

    public LoadingCache<Long,Release> getReleaseHistories() {
      return releaseHistories;
    }

    //新增一条
    public void addOne(String key){
      ConfigCacheEntry configCacheEntry=configCache.getUnchecked(key);
      Release latestRelease=configCacheEntry.getRelease();
      long notificationId=configCacheEntry.getNotificationId();
      releaseHistories.put(notificationId,latestRelease);
    }
  }

  private class ConfigHistories {
    //[releasemessageId,release] 用于存放历史的releaseMap
    private LoadingCache<String, ConfigHistoriesCacheEntry> configHistoriesCache;


    public ConfigHistories() {
      configHistoriesCache = CacheBuilder.newBuilder()
              .maximumSize(100)
              .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS, TimeUnit.SECONDS).build(new CacheLoader<String, ConfigHistoriesCacheEntry>() {
                @Override
                public ConfigHistoriesCacheEntry load(String key) throws Exception {
                  return new ConfigHistoriesCacheEntry(bizConfig.configServiceHistoryCacheHistoryMaxSize());
                }
              });
    }

    public LoadingCache<Long, Release> getReleaseHistories(String key) {
      return configHistoriesCache.getUnchecked(key).getReleaseHistories();
    }


    //新增一条
    public void addOne(String key){
      ConfigHistoriesCacheEntry configHistoriesCacheEntry=configHistoriesCache.getUnchecked(key);
      configHistoriesCacheEntry.addOne(key);
    }
  }

}
