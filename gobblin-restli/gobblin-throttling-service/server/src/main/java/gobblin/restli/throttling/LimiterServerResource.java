/*
 * Copyright (C) 2014-2017 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.restli.throttling;

import com.google.inject.Inject;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.ComplexKeyResourceTemplate;

import gobblin.broker.iface.NotConfiguredException;
import gobblin.broker.iface.SharedResourcesBroker;
import gobblin.util.limiter.Limiter;
import gobblin.util.limiter.broker.SharedLimiterFactory;
import gobblin.util.limiter.broker.SharedLimiterKey;


@RestLiCollection(name = "permits", namespace = "gobblin.restli.throttling")
public class LimiterServerResource extends ComplexKeyResourceTemplate<PermitRequest, EmptyRecord, PermitAllocation> {

  @Inject
  SharedResourcesBroker broker;

  @Override
  public PermitAllocation get(ComplexResourceKey<PermitRequest, EmptyRecord> key) {
    try {

      PermitRequest request = key.getKey();

      Limiter limiter =
          (Limiter) this.broker.<Limiter, SharedLimiterKey>getSharedResource(new SharedLimiterFactory(),
              new SharedLimiterKey(request.getResource()));

      limiter.acquirePermits(request.getPermits());

      PermitAllocation allocation = new PermitAllocation();
      allocation.setPermits(key.getKey().getPermits());
      allocation.setExpiration(Long.MAX_VALUE);
      return allocation;
    } catch (NotConfiguredException | InterruptedException nce) {
      throw new RuntimeException(nce);
    }
  }
}
