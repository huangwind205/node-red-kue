
module.exports = function (RED) {
    "use strict";

    const kue = require('kue');

    function KueNode(n) {
        RED.nodes.createNode(this, n);

        this.queueName = n.queueName;
        this.queuePrefix = n.queuePrefix || 'q';
        this.redisPort = n.redisPort || 6379;
        this.redisHost = n.redisHost || 'localhost';

        var node = this;

        const queue = kue.createQueue({
            prefix: this.queuePrefix,
            redis: {
                port: this.redisPort,
                host: this.redisHost
            }
        });

        node.status({ fill: "green", shape: "dot", text: "wait" });

        let statusQueues = {};

        queue.process(node.queueName, function (job, done) {

            node.status({ fill: "yellow", shape: "dot", text: "proccessing ... " });

            statusQueues[node.queueName] = done;

            node.send({
                payload:job, 
                done: (err, data) => {
                    done(err, data);
                    delete statusQueues[node.queueName];
                    node.status({ fill: "green", shape: "dot", text: "wait" });
                }
            });
        });

        queue.on('error', function (err) {
            node.status({ fill: "red", shape: "dot", text: err });
        });

        node.on("close", function () {
            node.status({ fill: "red", shape: "dot", text: "killed" });

            for (let o in statusQueues) {
                statusQueues[o](new Error("KILLED"));
            }            
        });
    }

    RED.nodes.registerType("kue-process", KueNode);
}
