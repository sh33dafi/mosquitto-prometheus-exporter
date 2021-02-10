const http = require('http');
const mqtt = require('mqtt');
const client = mqtt.connect(process.env.BROKER_URL);

const counterTopics = [
    '$SYS/broker/bytes/received',
    '$SYS/broker/bytes/sent',
    '$SYS/broker/messages/received',
    '$SYS/broker/messages/sent',
    '$SYS/broker/publish/bytes/received',
    '$SYS/broker/publish/bytes/sent',
    '$SYS/broker/publish/messages/received',
    '$SYS/broker/publish/messages/sent',
    '$SYS/broker/publish/messages/dropped',
    '$SYS/broker/uptime',
    '$SYS/broker/clients/maximum',
    '$SYS/broker/clients/total'
];

const ignoredTopics = [
    '$SYS/broker/timestamp',
    '$SYS/broker/version',
    '$SYS/broker/clients/active',
    '$SYS/broker/clients/inactive'
];

let data = new Map();

const parseTopic = (topic) => topic.replace('$SYS/', '').replaceAll(/[\s_\-./]/ig, '_');

const parseValue = (message) => parseFloat(message);

const processCounterMetric = (topic, message) => {
    const metric = {
        type: 'counter',
        value: parseValue(message)
    };
    data.set(topic, metric);
};
const processGaugeMetric = (topic, message) => {
    const metric = {
        type: 'gauge',
        value: parseValue(message)
    };
    data.set(topic, metric);
};

client.on('message', (topic, message) => {
    message = message.toString('utf8');
    const shouldPorcessTopic = !ignoredTopics.includes(topic);
    const isCounterTopic = counterTopics.includes(topic);
    if (shouldPorcessTopic) {
        if (isCounterTopic) {
            processCounterMetric(topic, message);
        } else {
            processGaugeMetric(topic, message);
        }
    }

});

const requestListener = (req, res) => {
    res.writeHead(200);
    res.end(Array.from(data.entries())
        .map(([key, metricValue]) => {
            const metric = parseTopic(key);
            const {type, value} = metricValue;
            return `# HELP ${metric}.
# TYPE ${metric} ${type}
${metric} ${value}`;
    }).join('\n'));
};

client.on('connect', () => {
    client.subscribe(['$SYS/#'], (err, granted) => {
        console.debug(`Subscribed to ${granted.map(granted => granted.topic)}`);
    });

    const server = http.createServer(requestListener);
    server.listen(3000).on('listening', () => console.debug('listening on port 3000'));
});

async function closeGracefully(signal) {
    console.debug(`Received signal to terminate: ${signal}`)
    client.end(true, {}, () => {
        process.exit()
    });
}
process.on('SIGINT', closeGracefully)
process.on('SIGTERM', closeGracefully)
