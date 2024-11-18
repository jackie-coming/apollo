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
package com.ctrip.framework.apollo.configservice.dto;

import com.ctrip.framework.apollo.common.entity.EntityPair;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import com.ctrip.framework.apollo.common.entity.KVEntity;
import com.ctrip.framework.apollo.configservice.enums.ChangeType;

public class ReleaseCompareResultDTO {

  private List<ChangeDTO> changeDTOSs = new LinkedList<>();

  public void addEntityPair(ChangeType type, KVEntity firstEntity, KVEntity secondEntity) {
    changeDTOSs.add(new ChangeDTO(type, new EntityPair<>(firstEntity, secondEntity)));
  }

  public List<KVEntity> getFirstEntitys() {
    return changeDTOSs.stream().map(ChangeDTO::getEntity).map(EntityPair::getFirstEntity).collect(Collectors.toList());
  }


}
