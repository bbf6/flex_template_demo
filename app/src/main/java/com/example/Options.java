package com.example;

import org.apache.beam.sdk.options.PipelineOptions;

public interface Options extends PipelineOptions {
  String getJob();
  void setJob(String value);

  String getSourceHost();
  void setSourceHost(String value);

  String getSourcePort();
  void setSourcePort(String value);

  String getSourceName();
  void setName(String value);

  String getSourceUser();
  void setUser(String value);

  String getSourcePassword();
  void setPassword(String value);

  String getTargetHost();
  void setTargetHost(String value);

  String getTargetPort();
  void setTargetPort(String value);

  String getTargetName();
  void setName(String value);

  String getTargetUser();
  void setUser(String value);

  String getTargetPassword();
  void setPassword(String value);
}
