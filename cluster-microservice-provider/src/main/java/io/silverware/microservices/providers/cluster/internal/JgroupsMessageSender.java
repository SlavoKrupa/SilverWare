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
package io.silverware.microservices.providers.cluster.internal;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.util.Buffer;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Class implementing functionality for sending messages
 *
 * @author Slavomír Krupa (slavomir.krupa@gmail.com)
 */
public class JgroupsMessageSender {
   /**
    * Logger.
    */
   private static final Logger log = LogManager.getLogger(JgroupsMessageSender.class);

   private static final RequestOptions SYNC_OPTIONS = RequestOptions.SYNC();
   private static final RequestOptions ASYNC_OPTIONS = RequestOptions.ASYNC();

   private final MessageDispatcher dispatcher;
   private Set<Address> filteredAddresses;

   public JgroupsMessageSender(MessageDispatcher dispatcher) {
      if (dispatcher == null) {
         throw new IllegalArgumentException("dispatcher");
      }
      this.dispatcher = dispatcher;
   }

   private Set<Address> getFilteredAddresses() {
      if (this.filteredAddresses == null) {
         this.filteredAddresses = new HashSet<>();
         // add my address
         filteredAddresses.add(this.dispatcher.getChannel().getAddress());
      }
      return this.filteredAddresses;
   }

   /**
    * Send multicast message to all nodes in cluster
    *
    * @param content
    *       content of message
    */
   public RspList sendToClusterSync(Serializable content) throws Exception {
      return this.dispatcher.castMessage(getMembersAddresses(), createBufferFromObject(content), SYNC_OPTIONS);
   }

   /**
    * Send async multicast message to all nodes in cluster
    *
    * @param content
    *       content of message
    */
   public <T> CompletableFuture<RspList<T>> sendToClusterAsync(Serializable content, Set<Address> addressesToSkip) throws Exception {
      List<Address> otherMembersAddresses = getOtherMembersAddresses().stream().filter(address -> !addressesToSkip.contains(address)).collect(Collectors.toList());
      if (!otherMembersAddresses.isEmpty()) {
         return this.dispatcher.castMessageWithFuture(otherMembersAddresses, createBufferFromObject(content), SYNC_OPTIONS);
      } else {
         if (log.isDebugEnabled()) {
            log.debug("No message sent.");
         }
      }

      return CompletableFuture.completedFuture(new RspList<T>());
   }

   /**
    * Send async multicast message to all nodes in cluster
    *
    * @param content
    *       content of message
    */
   public <T> CompletableFuture<RspList<T>> sendToClusterAsync(Serializable content) throws Exception {
      return sendToClusterAsync(content, Collections.emptySet());
   }

   private List<Address> getMembersAddresses() {
      return this.dispatcher.getChannel().getView().getMembers();
   }

   private List<Address> getOtherMembersAddresses() {
      Set<Address> filteredAddresses = getFilteredAddresses();
      return this.getMembersAddresses().stream().filter(address -> !filteredAddresses.contains(address)).collect(Collectors.toList());
   }

   /**
    * Send unicast message for specific address
    *
    * @param content
    *       content of message
    */
   public void sendToAddressAsync(Address address, Serializable content) throws Exception {
      this.dispatcher.sendMessage(address, createBufferFromObject(content), ASYNC_OPTIONS);
   }

   /**
    * Send unicast message for specific address
    *
    * @param content
    *       content of message
    */
   public <T> T sendToAddressSync(Address address, Serializable content) throws Exception {
      return this.dispatcher.sendMessage(address, createBufferFromObject(content), SYNC_OPTIONS);
   }

   private Buffer createBufferFromObject(final Serializable content) throws Exception {
      byte[] bytes = Util.objectToByteBuffer(content);
      return new Buffer(bytes, 0, bytes.length);
   }

}
