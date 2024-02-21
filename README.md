# Kafka Build Trigger

## Introduction

通过配置可以启动监听Kafka的 topic，通过消息来触发以`JJB_`作为前缀命名的Jenkins Job，比如`JJB_Test`

* 触发消息如下：

```json
{
	"project": "JJB_Test",
	"parameter": [
		{
			"name": "STR",
			"value": "xxxxxx"
		}
	]
}
```

* Jenkins System 配置如下图
  * broker是配置server url，格式`ip:port`
  * Topic：配置监听的topic name
  * Group Id：可以用于多个Jenkins同时消费消息

![image-20240221223758284](/Users/soloking/Library/Application Support/typora-user-images/image-20240221223758284.png)

## Getting started

TODO Tell users how to configure your plugin here, include screenshots, pipeline examples and 
configuration-as-code examples.

