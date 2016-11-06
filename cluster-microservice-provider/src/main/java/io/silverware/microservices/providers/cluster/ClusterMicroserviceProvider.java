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

import static io.silverware.microservices.providers.cluster.internal.exception.SilverWareClusteringException.SilverWareClusteringError.INITIALIZATION_ERROR;
import static io.silverware.microservices.providers.cluster.internal.exception.SilverWareClusteringException.SilverWareClusteringError.JGROUPS_ERROR;
import static java.util.Collections.emptySet;

import io.silverware.microservices.Context;
import io.silverware.microservices.MicroserviceMetaData;
import io.silverware.microservices.providers.MicroserviceProvider;
import io.silverware.microservices.providers.cluster.internal.JgroupsMessageReceiver;
import io.silverware.microservices.providers.cluster.internal.JgroupsMessageSender;
import io.silverware.microservices.providers.cluster.internal.exception.SilverWareClusteringException;
import io.silverware.microservices.providers.cluster.internal.message.KnownImplementation;
import io.silverware.microservices.providers.cluster.internal.message.response.MicroserviceSearchResponse;
import io.silverware.microservices.silver.ClusterSilverService;
import io.silverware.microservices.silver.cluster.RemoteServiceHandlesStore;
import io.silverware.microservices.silver.cluster.ServiceHandle;

import com.google.common.base.Stopwatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.util.RspList;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This provider provides a remote method invocation via JGroups.
 *
 * @author Slavomir Krupa (slavomir.krupa@gmail.com)
 * @author <a href="mailto:marvenec@gmail.com">Martin Večeřa</a>
 */
public class ClusterMicroserviceProvider implements MicroserviceProvider, ClusterSilverService {

   private static final Logger log = LogManager.getLogger(ClusterMicroserviceProvider.class);

   private Context context;
   private RemoteServiceHandlesStore remoteServiceHandlesStore;
   private Map<MicroserviceMetaData, Set<Address>> alreadyQueriedAddresses = new HashMap<>();

   private JgroupsMessageSender sender;
   private MessageDispatcher messageDispatcher;
   private Long timeout = 500L;

   @Override
   public void initialize(final Context context) {
      try {
         final Stopwatch stopwatch = Stopwatch.createStarted();
         // do some basic initialization
         this.context = context;
         this.remoteServiceHandlesStore = context.getRemoteServiceHandlesStore();
         this.alreadyQueriedAddresses = new HashMap<>();
         context.getProperties().putIfAbsent(CLUSTER_GROUP, "SilverWare");
         context.getProperties().putIfAbsent(CLUSTER_CONFIGURATION, "udp.xml");
         context.getProperties().putIfAbsent(CLUSTER_LOOKUP_TIMEOUT, timeout);
         // get jgroups configuration
         final String clusterGroup = (String) this.context.getProperties().get(CLUSTER_GROUP);
         final String clusterConfiguration = (String) this.context.getProperties().get(CLUSTER_CONFIGURATION);
         this.timeout = Long.valueOf(this.context.getProperties().get(CLUSTER_LOOKUP_TIMEOUT).toString());
         log.info("Hello from Cluster microservice provider!");
         log.info("Loading cluster configuration from: {} ", clusterConfiguration);
         JChannel channel = new JChannel(clusterConfiguration);
         JgroupsMessageReceiver receiver = new JgroupsMessageReceiver(KnownImplementation.initializeReponders(context), remoteServiceHandlesStore);
         channel.setReceiver(receiver);
         log.info("Setting cluster group: {} ", clusterGroup);
         channel.connect(clusterGroup);
         this.messageDispatcher = new MessageDispatcher(channel, receiver);
         this.sender = new JgroupsMessageSender(this.messageDispatcher);
         receiver.setMyAddress(channel.getAddress());
         stopwatch.stop();
         log.info("Initialization of ClusterMicroserviceProvider took {} ms. ", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      } catch (Exception e) {
         log.error("Cluster microservice initialization failed.", e);
         throw new SilverWareClusteringException(INITIALIZATION_ERROR, e);
      }
   }

   @Override
   public Context getContext() {
      return context;
   }

   @Override
   public void run() {
      try {
         while (!Thread.currentThread().isInterrupted()) {

         }
      } catch (final Exception e) {
         log.error("Cluster microservice provider failed.", e);
      } finally {
         log.info("Bye from Cluster microservice provider!");
         try {
            this.messageDispatcher.close();
         } catch (IOException e) {
            throw new SilverWareClusteringException(JGROUPS_ERROR, "Unexpected error while closing MessageDispatcher", e);
         }
      }
   }

   @Override
   public Set<Object> lookupMicroservice(final MicroserviceMetaData metaData) {
      try {
         Set<Address> addressesForMetadata = alreadyQueriedAddresses.getOrDefault(metaData, new HashSet<>());
         CompletableFuture<RspList<MicroserviceSearchResponse>> completableFuture = this.sender.sendToClusterAsync(metaData, addressesForMetadata);
         completableFuture.whenCompleteAsync((result, exception) -> {
            if (exception != null) {
               log.error("Error while looking up microservices.", exception);
               return;
            }
            try {
               if (log.isTraceEnabled()) {
                  log.trace("Response retrieved!  {}", result);
               }
               if (log.isTraceEnabled()) {
                  log.trace("Size of a responses is : {} ", result.getResults().size());
               }

               result.entrySet().stream().filter((entry) -> entry.getValue().hasException())
                     .forEach(entry -> log.error("Exception was thrown during lookup on other node {} : " + entry.getKey(), entry.getValue().getException()));
               Set<ServiceHandle> remoteServiceHandles = result.entrySet().stream()
                                                               .filter(entry -> entry.getValue().wasReceived() && !entry.getValue().hasException() && entry.getValue().getValue().canBeUsed())
                                                               .map((entry) -> new RemoteServiceHandle(entry.getKey(), entry.getValue().getValue().getHandle(), sender, metaData))
                                                               .collect(Collectors.toSet());
               // this is to save jgroups traffic for a given metadata
               addressesForMetadata.addAll(result.entrySet().stream().filter(entry -> !entry.getValue().hasException()).map(Map.Entry::getKey).collect(Collectors.toSet()));
               alreadyQueriedAddresses.put(metaData, addressesForMetadata);
               this.remoteServiceHandlesStore.addHandles(metaData, remoteServiceHandles);
            } catch (Throwable e) {
               log.error("Error while looking up microservices.", e);
            }

         });
         // If this is first query for the metadata we should wait for a response
         if (addressesForMetadata.isEmpty()) {
            Thread.sleep(timeout);
         }

         return this.remoteServiceHandlesStore.getServices(metaData);
      } catch (Throwable e) {
         log.error("Error while looking up microservices.", e);
         return emptySet();
      }
   }

   @Override
   public Set<Object> lookupLocalMicroservice(final MicroserviceMetaData metaData) {
      return emptySet();
   }

}