/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;

import java.util.Optional;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Since Guice recommends to avoid injecting closeable resources (see
 * https://github.com/google/guice/wiki/Avoid-Injecting-Closable-Resources), this factory creates
 * short lived clients that can be closed independently.
 */
public class BigQueryReadClientFactory implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(BigQueryReadClientFactory.class);

  private final Credentials credentials;
  // using the user agent as HeaderProvider is not serializable
  private final UserAgentHeaderProvider userAgentHeaderProvider;
  private static final String DEFAULT = new String("DEFAULT");
  // Used for serializibility.  This is expected to be empty when Spark needs to serialized
  // it so it should be safe despite TransportChannelProvider not being serializable
  private final HashMap<String, BigQueryReadClient> clients = new HashMap<>();

  @Inject
  public BigQueryReadClientFactory(
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier,
      UserAgentHeaderProvider userAgentHeaderProvider) {
    // using Guava's optional as it is serializable
    this.credentials = bigQueryCredentialsSupplier.getCredentials();
    this.userAgentHeaderProvider = userAgentHeaderProvider;
  }

  private BigQueryReadClient newClient(String endpoint) throws IOException {
    InstantiatingGrpcChannelProvider.Builder transportBuilder =
        BigQueryReadSettings.defaultGrpcTransportProviderBuilder()
            .setHeaderProvider(userAgentHeaderProvider)
            .setChannelsPerCpu(.1);
    if (!endpoint.equals(DEFAULT)) {
      log.info("Overriding endpoint to: ", endpoint);
      transportBuilder.setEndpoint(endpoint);
    }
    BigQueryReadSettings.Builder clientSettings =
        BigQueryReadSettings.newBuilder()
            .setTransportChannelProvider(transportBuilder.build())
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials));
    return BigQueryReadClient.create(clientSettings.build());
  }

  public synchronized BigQueryReadClient createBigQueryReadClient(Optional<String> endpoint) {
    try {
      String endpointKey = endpoint.orElse(DEFAULT);
      BigQueryReadClient client = null;
      synchronized (clients) {
        client = clients.get(endpointKey);
        if (client == null) {
          client = newClient(endpointKey);
          clients.put(endpointKey, client);
        }
      }
      return client;
    } catch (IOException e) {
      throw new UncheckedIOException("Error creating BigQueryStorageClient", e);
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    // This object needs to be serializable but clients aren't.  Its not
    // too important to keep the same clients so simply clear them, serialize
    // and then restore them.
    HashMap<String, BigQueryReadClient> existing = new HashMap<>();
    synchronized (clients) {
      existing.putAll(clients);
      clients.clear();
      out.defaultWriteObject();
      clients.putAll(existing);
    }
  }
}
