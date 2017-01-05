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

package gobblin.broker;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import gobblin.broker.gobblin_scopes.GobblinScopeTypes;
import gobblin.broker.gobblin_scopes.JobScopeInstance;
import gobblin.broker.gobblin_scopes.TaskScopeInstance;


public class AutoscopedFactoryTest {

  @Test
  public void testAutoscoping() throws Exception {
    Config config = ConfigFactory.empty();

    config = TestFactory.setAutoScopeLevel(config, GobblinScopeTypes.JOB);

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("myJob", "job123")).build();
    SharedResourcesBrokerImpl<GobblinScopeTypes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopeTypes.CONTAINER.defaultScopeInstance()).build();
    SharedResourcesBrokerImpl<GobblinScopeTypes> taskBroker = jobBroker.newSubscopedBuilder(new TaskScopeInstance("taskabc"))
        .withAdditionalParentBroker(containerBroker).build();

    TestFactory.SharedResource jobScopedResource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopeTypes>(), new TestResourceKey("myKey"), GobblinScopeTypes.JOB);
    TestFactory.SharedResource taskScopedResource =
        taskBroker.getSharedResourceAtScope(new TestFactory<GobblinScopeTypes>(), new TestResourceKey("myKey"), GobblinScopeTypes.TASK);
    TestFactory.SharedResource autoscopedResource =
        taskBroker.getSharedResource(new TestFactory<GobblinScopeTypes>(), new TestResourceKey("myKey"));

    Assert.assertEquals(jobScopedResource, autoscopedResource);
    Assert.assertNotEquals(taskScopedResource, autoscopedResource);
  }

  @Test
  public void testAutoscopedResourcesOnlyClosedInCorrectScope() throws Exception {
    Config config = ConfigFactory.empty();

    config = TestFactory.setAutoScopeLevel(config, GobblinScopeTypes.JOB);

    SharedResourcesBrokerImpl<GobblinScopeTypes> topBroker = SharedResourcesBrokerFactory.createDefaultTopLevelBroker(config,
        GobblinScopeTypes.GLOBAL.defaultScopeInstance());
    SharedResourcesBrokerImpl<GobblinScopeTypes> jobBroker =
        topBroker.newSubscopedBuilder(new JobScopeInstance("myJob", "job123")).build();
    SharedResourcesBrokerImpl<GobblinScopeTypes>
        containerBroker = topBroker.newSubscopedBuilder(GobblinScopeTypes.CONTAINER.defaultScopeInstance()).build();
    SharedResourcesBrokerImpl<GobblinScopeTypes> taskBroker = jobBroker.newSubscopedBuilder(new TaskScopeInstance("taskabc"))
        .withAdditionalParentBroker(containerBroker).build();

    TestFactory.SharedResource autoscopedResource =
        taskBroker.getSharedResource(new TestFactory<GobblinScopeTypes>(), new TestResourceKey("myKey"));

    // since object autoscopes at job level, it should not be closed if we close the task broker
    taskBroker.close();
    Assert.assertFalse(autoscopedResource.isClosed());

    // however, when closing job broker, resource should be closed
    jobBroker.close();
    Assert.assertTrue(autoscopedResource.isClosed());

  }

}
