"use strict"

var 
	kafka  = require('kafka-node'),
	uuid   = require('uuid');



var kafkaClient = new kafka.KafkaClient('http://localhost:2181', 'my-client-id', {
	sessionTimeout : 300,
	spinDelay : 100,
	retiries : 2
});


var producer =  new kafka.HighLevelProducer(kafkaClient);

producer.on('ready', function() {
	console.log("kafka producer is ready to produce message");
});

producer.on('error', function(error) {
	console.log("kafka producer generate error::", error);
});


const kafkaService = {
	sendRecords: ({type, userId, sessionId, data}, callback = ()=>{})=> {
		if(!userId) {
			return callback(new Error("userId is mandatory parameter it must be present"));
		}

		const event = {
			id : uuid.v4(),
			timestamp : new Date(),
			userId : userId,
			sessionId : sessionId,
			type : type,
			data : data
		}

		const buffer = new Buffer.from(JSON.stringify(event));

		var records = [{
			topic: "webevent.dev",
			messages : buffer,
			attributes : 1
		}];
		producer.send(records, callback);
	}
};



module.exports = kafkaService;
