
module.exports = function (RED) {
    "use strict";

    const kue = require('kue');

    function KueNode(n) {
        RED.nodes.createNode(this, n);

        this.queueName = n.queueName;
        this.queuePrefix = n.queuePrefix || 'q';
        this.redisPort = n.redisPort || 6379;
        this.redisHost = n.redisHost || 'localhost';
        this.attempts = n.attempts || 5;
        this.ttl = n.ttl || 5000;

        var node = this;

        const queue = kue.createQueue({
            prefix: this.queuePrefix,
            redis: {
                port: this.redisPort,
                host: this.redisHost
            }
        });

        this.on('input', msg => {
            queue.create(node.queueName, msg.payload)
                .priority('high')
                .attempts(node.attempts)
                .ttl(this.ttl)
                .save();
        });
    }

    RED.nodes.registerType("kue-queue", KueNode);
}
