package com.example.qdranthackathon;

import org.apache.beam.sdk.options.PipelineOptions;

public interface QdrantPipelineOptions extends PipelineOptions {

    String getBrokers();

    void setBrokers(String brokers);

    String getTopic();

    void setTopic(String topic);

    String getKafkaUsername();

    void setKafkaUsername(String username);

    String getKafkaPassword();

    void setKafkaPassword(String password);

    String getKafkaSecurityProtocol();

    void setKafkaSecurityProtocol(String securityProtocol);

    String getKafkaSaslMechanism();

    void setKafkaSaslMechanism(String saslMechanism);

    String getEmbeddingModel();

    void setEmbeddingModel(String model);

    String getOpenaiApiKey();

    void setOpenaiApiKey(String key);

    String getQdrantHost();

    void setQdrantHost(String host);

    String getQdrantApiKey();

    void setQdrantApiKey(String apikey);

}