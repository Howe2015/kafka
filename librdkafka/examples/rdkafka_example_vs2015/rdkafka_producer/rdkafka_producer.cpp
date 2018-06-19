#include "src-cpp/rdkafkacpp.h"
#include <iostream>
#include <string>
#include <thread>
#include <chrono>

int main()
{	
	std::string topics = "TEST";
	std::string brokers = "bd1:9092";
	std::string group = "1";
	int32_t partition = RdKafka::Topic::PARTITION_UA;
	//创建配置
	RdKafka::Conf *conf = nullptr;
	conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

	std::string errstr;
	std::string strfetch_num = "10240000";

	conf->set("metadata.broker.list", brokers, errstr);
	
	RdKafka::Producer *producer = RdKafka::Producer::create(conf,errstr);
	if (!producer) {
		std::cerr << "Failed to create producer: " << errstr << std::endl;
		exit(1);
	}
	std::cout << "% Created producer " << producer->name() << std::endl;

	//创建topic配置
	RdKafka::Conf *tconf = nullptr;
	tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	RdKafka::Topic *topic = RdKafka::Topic::create(producer, topics, tconf, errstr);
	if (!topic) {
		std::cerr << "Failed to create topic: " << errstr << std::endl;
		exit(1);
	}

	int num = 0;
	while (1)
	{
		RdKafka::ErrorCode resp = producer->produce(topic, partition,
			RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
			const_cast<char *>("hello world"), 11,
			NULL, NULL);

		producer->poll(0);
		std::this_thread::sleep_for(std::chrono::seconds(2));
		num++;
		if (num == 5)
			break;
	}

    return 0;
}

