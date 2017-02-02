
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

        node.statusQueues = {};

        node.qtde = node.qtde || 0;

        queue.process(node.queueName, function (job, done) {

            node.status({ fill: "yellow", shape: "dot", text: "proccessing ... " });

            node.statusQueues[node.queueName] = done;

            node.send({
                payload: job.data,
                done: (err, data) => {
                    done(err, data);
                    delete node.statusQueues[node.queueName];
                    node.qtde++;
                    node.status({ fill: "green", shape: "dot", text: node.qtde + " proccessed" });
                }
            });
        });

        queue.on('error', function (err) {
            node.status({ fill: "red", shape: "dot", text: err });
        });

        node.on("close", function () {
            node.status({ fill: "red", shape: "dot", text: "killed" });

            for (let o in node.statusQueues) {
                node.statusQueues[o](new Error("KILLED"));
            }

            queue.shutdown(0, function (err) {
                console.log('Kue shutdown: ', err || '');                
            });

        });
    }

    RED.nodes.registerType("kue-process", KueNode);
}
