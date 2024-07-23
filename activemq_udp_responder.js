const dgram = require('dgram')
const os = require('os')

const multicastAddress = '239.255.255.250'
const udpPort = 41234
const activeMqPort = 61613

// Função para obter o IP local
function getLocalIP() {
    const interfaces = os.networkInterfaces()
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                return iface.address
            }
        }
    }
    return '0.0.0.0'
}

const localIP = getLocalIP()

// Servidor UDP para responder a mensagens DISCOVER_ACTIVEMQ
const udpServer = dgram.createSocket({ type: 'udp4', reuseAddr: true })

udpServer.on('listening', () => {
    const address = udpServer.address()
    console.log(`UDP Server listening on ${address.address}:${address.port}`)
    udpServer.addMembership(multicastAddress)
})

udpServer.on('message', (msg, rinfo) => {
    if (msg.toString().includes('DISCOVER_ACTIVEMQ')) {
        const response = `Server IP: ${localIP}, ActiveMQ Port: ${activeMqPort}`
        udpServer.send(response, 0, response.length, rinfo.port, rinfo.address)
        console.log(`Sent response to ${rinfo.address}:${rinfo.port}`)
    }
})

udpServer.bind(udpPort, () => {
    console.log(`UDP Server bound to port ${udpPort}`)
})
