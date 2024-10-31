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
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


/**
 * config service with guava cache
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ConfigServiceWithChangeCache extends ConfigServiceWithCache implements IncrementalSyncConfigService {
  private static final Logger logger = LoggerFactory.getLogger(ConfigServiceWithChangeCache.class);

  private static final Gson GSON = new Gson();

  private static final long DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES = 60;

  private final Integer size=10;

  private LoadingCache<String, ConfigChangeCacheEntry> configChangeCache;

  private LoadingCache<String, ConfigHistoriesCacheEntry> configHistoriesCache;


  public ConfigServiceWithChangeCache(final ReleaseService releaseService,
                                      final ReleaseMessageService releaseMessageService,
                                      final GrayReleaseRulesHolder grayReleaseRulesHolder,
                                      final BizConfig bizConfig,
                                      final MeterRegistry meterRegistry) {
    super(releaseService,releaseMessageService,grayReleaseRulesHolder,bizConfig,meterRegistry);
  }

  @PostConstruct
  void initialize() {
    buildConfigHistoriesCache();
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
      configChangeCache.invalidate(messageKey);

      //warm up the cache
      configChangeCache.getUnchecked(messageKey);
    } catch (Throwable ex) {
      //ignore
    }
  }
  @Override
  public Map<String,String> findLatestActiveChangeConfigurations(String appId, String clusterName, String namespaceName,
                                                                 ApolloNotificationMessages clientMessages, long historyReleaseId) {
    String messageKey = ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName);
    String cacheKey = messageKey;

    if (bizConfig.isConfigServiceCacheKeyIgnoreCase()) {
      cacheKey = cacheKey.toLowerCase();
    }

    Tracer.logEvent(TRACER_EVENT_CACHE_GET, cacheKey);

    ConfigChangeCacheEntry configChangeCacheEntry = configChangeCache.getUnchecked(cacheKey);

    //cache is out-dated
    if (clientMessages != null && clientMessages.has(messageKey) &&
            clientMessages.get(messageKey) > configChangeCacheEntry.getLatestRelease().getId()) {
      //invalidate the cache and try to load from db again
      configChangeCache.invalidate(cacheKey);
      configChangeCacheEntry = configChangeCache.getUnchecked(cacheKey);
    }

    return configChangeCacheEntry.getConfigurationsByReleaseId(historyReleaseId);
  }

  private void buildConfigChangeCache() {
    CacheBuilder configChangeCacheBuilder = CacheBuilder.newBuilder()
            .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES, TimeUnit.MINUTES);
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
          return new ConfigChangeCacheEntry(null);
        }

        Transaction transaction = Tracer.newTransaction(TRACER_EVENT_CACHE_LOAD, key);
        try {
          configCache.invalidate(key);
          ConfigCacheEntry configCacheEntry=configCache.getUnchecked(key);
          Release latestRelease=configCacheEntry.getRelease();

          ConfigChangeCacheEntry configChangeCacheEntry=new ConfigChangeCacheEntry(latestRelease);

          configHistoriesCache.invalidate(key);
          ConfigHistoriesCacheEntry historiesCacheEntry = configHistoriesCache.getUnchecked(key);

          Map<Long,Release> releaseHistories=historiesCacheEntry.getReleaseHistories();
          Map<String,ReleaseCompareResultDTO> releaseChanges=new HashMap<>();

          for(Map.Entry<Long,Release> entry:releaseHistories.entrySet()){
            Release historiestrelease=entry.getValue();
            configChangeCacheEntry.addOne(historiestrelease);
          }

          transaction.setStatus(Transaction.SUCCESS);

          return configChangeCacheEntry;
        } catch (Throwable ex) {
          transaction.setStatus(ex);
          throw ex;
        } finally {
          transaction.complete();
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
  private void buildConfigHistoriesCache() {
    CacheBuilder configCacheBuilder = CacheBuilder.newBuilder()
        .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES, TimeUnit.MINUTES);
    if (bizConfig.isConfigServiceCacheStatsEnabled()) {
      configCacheBuilder.recordStats();
    }

    configHistoriesCache = configCacheBuilder.build(new CacheLoader<String, ConfigHistoriesCacheEntry>() {
      @Override
      public ConfigHistoriesCacheEntry load(String key) throws Exception {
        List<String> namespaceInfo = ReleaseMessageKeyGenerator.messageToList(key);
        if (CollectionUtils.isEmpty(namespaceInfo)) {
          Tracer.logError(
              new IllegalArgumentException(String.format("Invalid cache load key %s", key)));
          return new ConfigHistoriesCacheEntry(Maps.newHashMap());
        }

        Transaction transaction = Tracer.newTransaction(TRACER_EVENT_CACHE_LOAD, key);
        try {
          //TODO 配置化&& 配置过大 分页拉取
          PageRequest page = PageRequest.of(0, 10);

          List<Release> releases = releaseService.findActiveReleases(namespaceInfo.get(0), namespaceInfo.get(1),
                                                                         namespaceInfo.get(2),page);

          Map<Long,Release> releaseHistories= releases.stream().collect(Collectors.toMap(Release::getId, Function.identity()));

          transaction.setStatus(Transaction.SUCCESS);


          return new ConfigHistoriesCacheEntry(releaseHistories);
        } catch (Throwable ex) {
          transaction.setStatus(ex);
          throw ex;
        } finally {
          transaction.complete();
        }
      }
    });

  }


  private void addChange(String key,long historyReleaseId){
    ConfigCacheEntry latestcacheEntry = configCache.getUnchecked(key);
    ConfigHistoriesCacheEntry historiesCacheEntry = configHistoriesCache.getUnchecked(key);
    ConfigChangeCacheEntry configChangeCacheEntry = configChangeCache.getUnchecked(key);
    //从db读取releaseId
    Release historyRelease=releaseService.findOne(historyReleaseId);
    long removeReleaseId=0;
    if(historiesCacheEntry.getReleaseHistories().size()>=size){
      removeReleaseId=historiesCacheEntry.removeOne();
    }
    historiesCacheEntry.addOne(historyRelease);
    configChangeCacheEntry.removeByReleaseId(removeReleaseId);
    configChangeCacheEntry.addOne(historyRelease);

  }

  private  class ConfigChangeCacheEntry {
    private final Map<String,ReleaseCompareResultDTO> releaseChanges;

    private final Release latestRelease;
    public ConfigChangeCacheEntry(Release latestRelease) {
      this.releaseChanges = new HashMap<>();
      this.latestRelease=latestRelease;
    }

    public Map<String,ReleaseCompareResultDTO> getReleaseChanges() {
      return releaseChanges;
    }
    public Release getLatestRelease() {
      return latestRelease;
    }


    private void addOne(Release historyRelease){
      ReleaseCompareResultDTO releaseCompareResultDTO;
      if(latestRelease.getId()>=historyRelease.getId()) {
        releaseCompareResultDTO=compare(historyRelease,latestRelease);
      }else{
        releaseCompareResultDTO=compare(latestRelease,historyRelease);
      }
      releaseChanges.put(getChangeKey(historyRelease.getId()),releaseCompareResultDTO);
    }
    private void removeByReleaseId(long historiesReleaseId){
      releaseChanges.remove(getChangeKey(historiesReleaseId));
    }

    private Map<String,String> getConfigurationsByReleaseId(long historiesReleaseId){
      ReleaseCompareResultDTO releaseCompareResultDTO=releaseChanges.get(getChangeKey(historiesReleaseId));
      if(latestRelease.getId()>=historiesReleaseId){
        return releaseCompareResultDTO.getSecondEntitys().stream().collect(Collectors.toMap(KVEntity::getKey, KVEntity::getValue));
      }
      return releaseCompareResultDTO.getFirstEntitys().stream().collect(Collectors.toMap(KVEntity::getKey, KVEntity::getValue));
    }
    //getChangeKey historiestrelease和latestRelease.getId() 的id按照大-小的顺序 拼接得到新的changeKey
    private String getChangeKey(long historiesReleaseId){
      String changeKey;
      if(historiesReleaseId>=latestRelease.getId()) {
        changeKey=historiesReleaseId+"+"+latestRelease.getId();
      }else{
        changeKey= latestRelease.getId()+"+"+historiesReleaseId;
      }
      return changeKey;
    }

  }

  private static class ConfigHistoriesCacheEntry {
    //TODO 加入缓存淘汰的功能
    //[releasemessageId,release] 用于存放历史的releaseMap
    //TODO 并发安全
    private final Map<Long,Release> releaseHistories;

    public ConfigHistoriesCacheEntry(Map<Long,Release> releaseHistories) {
      this.releaseHistories = releaseHistories;
    }

    public Map<Long,Release> getReleaseHistories() {
      return releaseHistories;
    }
    //根据id删除
    public long removeOne(){
      //删除key最小的
      Long releaseId=releaseHistories.keySet().stream().min(Long::compare).get();
      releaseHistories.remove(releaseId);
      return releaseId;
    }
    //新增一条
    public void addOne(Release release){
      releaseHistories.put(release.getId(),release);
    }

  }

}
