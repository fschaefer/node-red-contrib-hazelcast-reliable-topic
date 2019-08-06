
enum NodeType {
	ReliableTopicInputNode,
	ReliableTopicOutputNode
}

let Client = require('hazelcast-client').Client
let Config = require('hazelcast-client').Config

module.exports = function (RED: any) {

	function nodeFactory(type: NodeType) {
		return function HazelcastReliableTopicNode(this: any, config: any) {

			RED.nodes.createNode(this, config)

			let serverConfig = RED.nodes.getNode(config.server)

			let hazelcastConfig = new Config.ClientConfig()

			hazelcastConfig.groupConfig.name = config.group_name
			hazelcastConfig.groupConfig.password = config.group_password

			;(serverConfig.hosts || ["localhost:5701"]).split(/\s*,\s*/).forEach((host: string) => {
				hazelcastConfig.networkConfig.addresses.push(host)
			})

            hazelcastConfig.networkConfig.connectionTimeout = serverConfig.connectionTimeout
            hazelcastConfig.networkConfig.connectionAttemptPeriod = serverConfig.connectionAttemptPeriod
            hazelcastConfig.networkConfig.connectionAttemptLimit = serverConfig.connectionAttemptLimit


			Client.newHazelcastClient(hazelcastConfig).then((client: any) => {
				client.getReliableTopic(config.topic).then((topic: any) => {
					if (type == NodeType.ReliableTopicInputNode) {
						topic.addMessageListener((message: any) => {
							let msg = {
								"payload": message.messageObject,
								"publisher": message.publisher,
								"publishingTime": message.publishingTime
							}
							try {
								msg.payload = JSON.parse(message.messageObject)
							}
							catch(e) {}
							this.send(msg)
						})
					}
					else if (type == NodeType.ReliableTopicOutputNode) {
						this.on('input', (message: any) => {
							topic.publish(JSON.stringify(message.payload))
						})
					}
				})
			})
		}
	}

    RED.nodes.registerType("hazelcast-reliable-topic-input-node", nodeFactory(NodeType.ReliableTopicInputNode))
    RED.nodes.registerType("hazelcast-reliable-topic-output-node", nodeFactory(NodeType.ReliableTopicOutputNode))


    function RemoteServerNode(this: any, config: any) {
        RED.nodes.createNode(this, config)
        this.hosts = config.hosts
        this.connectionTimeout = config.connection_timeout
        this.connectionAttemptPeriod = config.connection_attempt_period
        this.connectionAttemptLimit = config.connection_attempt_limit
    }

    RED.nodes.registerType("hazelcast-remote-server", RemoteServerNode)
}

