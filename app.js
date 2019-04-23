"use strict"

var 
	kafkaProducer = require('./kafkaProducer.js'),
	fs 			  = require('fs');



fs.readFile('./messages.txt', 'utf8', function(error, content) {
	sendMessages(content.split(' '));
});


function sendMessages(messageArray, index = 0) {
	setTimeout(function() {
		var 
			type = "test",
			sessionId = new Date().getTime(),
			userId  = index+1,
			data = messageArray[index];
			kafkaProducer.sendRecords({
			type, sessionId, userId, data
			}, (error) => {
				console.log("kafka producer callabck::", error);
			});

		if(index < messageArray.length) {
			sendMessages(messageArray, index + 1);	
		}
	}, 1000)
}