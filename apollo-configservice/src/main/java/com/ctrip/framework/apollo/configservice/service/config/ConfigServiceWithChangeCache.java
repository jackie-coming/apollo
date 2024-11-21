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
import com.ctrip.framework.apollo.biz.grayReleaseRule.GrayReleaseRulesHolder;
import com.ctrip.framework.apollo.biz.service.ReleaseMessageService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.common.entity.KVEntity;
import com.ctrip.framework.apollo.configservice.dto.ReleaseCompareResultDTO;
import com.ctrip.framework.apollo.configservice.enums.ChangeType;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * config service with change cache
 *
 * @author Jason
 */
public class ConfigServiceWithChangeCache implements IncrementalSyncConfigService {
  private static final Logger logger = LoggerFactory.getLogger(ConfigServiceWithChangeCache.class);


  private static final long DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS = 10;


  public LoadingCache<String,Map<String, String>> mergedReleaseCache;


  public ConfigServiceWithChangeCache() {}

  @PostConstruct
  public void initialize() {
    buildMergedReleaseCache();
  }

  private void buildMergedReleaseCache() {
    CacheBuilder mergedReleaseCacheBuilder = CacheBuilder.newBuilder()
            .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_SencondS, TimeUnit.SECONDS);

    mergedReleaseCache = mergedReleaseCacheBuilder.build(new CacheLoader<String,Map<String, String>>() {
      @Override
      public Map<String, String> load(String key) throws Exception {
      return null;
      }
    });
  }

  @Override
  public  Map<String,String> changeConfigurations(Map<String, String> latestReleaseConfigurations, Map<String, String> historyReleaseConfigurations){
    //调用compare方法
    return compare(latestReleaseConfigurations, historyReleaseConfigurations).getFirstEntitys().stream().collect(Collectors.toMap(KVEntity::getKey, KVEntity::getValue));
  }

  @Override
  public  Map<String,String> findConfigurations(String mergedReleaseKey){
    //从缓存拿到配置
    Map<String,String> historyConfigurations = mergedReleaseCache.getIfPresent(mergedReleaseKey);
    if(historyConfigurations==null){
      //历史缓存找不到 1、过期 2、增量开关
      return null;
    }
    return historyConfigurations;
  }

  @Override
  public void cache(String latestMergedReleaseKey,Map<String, String> latestReleaseConfigurations){
    //保存到缓存,这部分数据从缓存 或者db 中拿都可以。 什么时候更新？ 用户请求的时候？ 监听message的消息？
    if(mergedReleaseCache.getIfPresent(latestMergedReleaseKey)==null){
      mergedReleaseCache.put(latestMergedReleaseKey, latestReleaseConfigurations);
    }
  }




  //base是旧的，toCompare是新的
  public ReleaseCompareResultDTO compare(Map<String, String> latestReleaseConfigurations, Map<String, String> historyReleaseConfigurations) {

    ReleaseCompareResultDTO compareResult = new ReleaseCompareResultDTO();

    //added and modified in latestReleaseConfigurations
    for (Map.Entry<String, String> entry : latestReleaseConfigurations.entrySet()) {
      String key = entry.getKey();
      String firstValue = entry.getValue();
      String secondValue = historyReleaseConfigurations.get(key);
      //added
      if (secondValue == null) {
        compareResult.addEntityPair(ChangeType.ADDED, new KVEntity(key, firstValue),
                                    new KVEntity(key, null));
      } else if (!Objects.equal(firstValue, secondValue)) {
        compareResult.addEntityPair(ChangeType.MODIFIED, new KVEntity(key, firstValue),
                                    new KVEntity(key, secondValue));
      }

    }

    //deleted in latestReleaseConfigurations
    for (Map.Entry<String, String> entry : historyReleaseConfigurations.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (latestReleaseConfigurations.get(key) == null) {
        compareResult.addEntityPair(ChangeType.DELETED, new KVEntity(key,""), new KVEntity(key, value));
      }

    }

    return compareResult;
  }
}
