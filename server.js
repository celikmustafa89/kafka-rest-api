var http = require('http');
var express = require('express');
var bodyParser = require('body-parser');

var cities = [{name: 'Istanbul', country: 'Turkey'}, {name: 'New York', country: 'USA'},
    {name: 'London', country: 'England'}];

var app = express();

app.use(bodyParser.urlencoded({extended: true}));
app.use(bodyParser.json());

app.listen(1923, async function () {
    console.log("Port dinleniyor 1923...");
});


let payloads = [
    {
        topic: 'test',
        messages: "mustafa"
    }
];



// kafka part
/////////////
const kafka = require('kafka-node');
const kafka_topic = 'test';
try {
    const Producer = kafka.Producer;
    const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
    var producer = new Producer(client);

    console.log("Producer Initialised..");



    producer.on('ready', function() {
        producer.send(payloads, (err, data) => {
            if (err) {
                console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
            } else {
                console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
            }
        });

    });

    producer.on('error', function(err) {
        console.log(err);
        console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
        throw err;
    });
}
catch(e) {
    console.log(e);
}

/////////////

app.post('/event', async function (request, response) {
    response.send("ok");
    payloads[0].messages = request.body["event"];
    producer.send(payloads, (err, data) => {
        if (err) {
            console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
            //response.send("kafka push failed");
        } else {
            console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
            //response.send("kafka push successful");
        }
    });
    console.log("cevap verildi.")
});
