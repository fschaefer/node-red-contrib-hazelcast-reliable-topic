
//https://github.com/hazelcast/hazelcast-nodejs-client/blob/95e1d1f5db1644ba647244925576703b2a6bd285/README.md#55-setting-connection-attempt-limit
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

			this.topic = config.topic || ""
			this.groupName = config.group_name || ""
			this.groupPassword = config.group_password || ""
			this.server = RED.nodes.getNode(config.server)
			this.server.hosts = this.server.hosts || "localhost:5701"

			let hazelcastConfig = new Config.ClientConfig()

			;(this.server.hosts.split(/\s*,\s*/) || []).forEach((host: string) => {
				hazelcastConfig.networkConfig.addresses.push(host)
			})
			
			hazelcastConfig.groupConfig.name = this.groupName
			hazelcastConfig.groupConfig.password = this.groupPassword

			Client.newHazelcastClient(hazelcastConfig).then((client: any) => {
				client.getReliableTopic(this.topic).then((topic: any) => {
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
    }

    RED.nodes.registerType("hazelcast-remote-server", RemoteServerNode)
}

