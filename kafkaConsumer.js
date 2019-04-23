"use strict"
var 
	kafka = require('kafka-node'),
	client = new kafka.KafkaClient('http://localhost:2181');

const topics = [{
	topic : "webevent.dev"
}]

const options = {
	"autoCommit" : true,
	"fetchMaxWaitMs" : 1000,
	"fetchMaxBytes" : 1024*1024,
	"encoding" : "buffer"
};

var consumer = new kafka.Consumer(client, topics, options);


consumer.on("message", function(message) {
	var decodeMessage  = message;
	try {
		//var buffer = new Buffer(message.value, 'binary');
		//decodeMessage = JSON.parse(buffer.toString());
	}
	catch(ex) {
		console.log("exception");
	}
	console.log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
	console.log("New message come from Producer", new Date());
	console.log("message :: ", decodeMessage.value.toString());
	console.log("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
});

consumer.on('error', function(error) {
	console.log("Error occured in Consumer Side", error);
});

consumer.on("SIGINT", function() {
	consumer.close(true, function() {
		process.exit();
	})
})