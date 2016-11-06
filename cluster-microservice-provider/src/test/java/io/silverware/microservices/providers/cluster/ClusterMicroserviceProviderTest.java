/*
 * -----------------------------------------------------------------------\
 * SilverWare
 *  
 * Copyright (C) 2010 - 2016 the original author or authors.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -----------------------------------------------------------------------/
 */
package io.silverware.microservices.providers.cluster;

import static io.silverware.microservices.providers.cluster.internal.RemoteServiceHandleStoreTest.META_DATA;
import static org.assertj.core.api.Assertions.assertThat;

import io.silverware.microservices.providers.cluster.internal.JgroupsMessageSender;
import io.silverware.microservices.providers.cluster.internal.message.response.MicroserviceSearchResponse;
import io.silverware.microservices.silver.cluster.RemoteServiceHandlesStore;
import io.silverware.microservices.silver.cluster.ServiceHandle;

import org.jgroups.Address;
import org.jgroups.util.RspList;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Tested;

/**
 * Test for ClusterMicroserviceProvider
 *
 * @author Slavomír Krupa (slavomir.krupa@gmail.com)
 */
public class ClusterMicroserviceProviderTest {

   private Address address = new org.jgroups.util.UUID();

   @Tested
   private ClusterMicroserviceProvider clusterMicroserviceProvider;
   @Injectable
   private RemoteServiceHandlesStore remoteServiceHandlesStore;
   @Injectable
   private JgroupsMessageSender sender;

   @Test
   public void testLookupMicroservice() throws Exception {

      MicroserviceSearchResponse response = new MicroserviceSearchResponse(1, MicroserviceSearchResponse.Result.FOUND);
      RspList<MicroserviceSearchResponse> rspList = new RspList<>();
      rspList.addRsp(address, response);
      CompletableFuture<RspList<MicroserviceSearchResponse>> completableFuture = CompletableFuture.completedFuture(rspList);
      Set<Object> services = Util.createSetFrom(new Object(), new Object());

      new Expectations() {{
         sender.sendToClusterAsync((Serializable) any, (Set<Address>) any);
         times = 1;
         result = completableFuture;
         remoteServiceHandlesStore.addHandles(META_DATA, (Set<ServiceHandle>) any);
         times = 1;

         remoteServiceHandlesStore.getServices(META_DATA);
         result = services;
         times = 1;

      }};
      Set<Object> objects = clusterMicroserviceProvider.lookupMicroservice(META_DATA);
      assertThat(objects).isEqualTo(services);
   }

   @Test
   public void testLookupLocalMicroservice() throws Exception {
      Set<Object> objects = clusterMicroserviceProvider.lookupLocalMicroservice(META_DATA);
      assertThat(objects)
            .isNotNull()
            .isEmpty();

   }
}