const dgram = require('dgram')
const postgres = require('postgres')
const Stomp = require('stomp-client')

const multicastAddress = '239.255.255.250'
const udpPort = 41234
const clientPort = 41235
const downloadQueue = '/queue/download'
const downloadResponseQueue = '/queue/download-response'
const listFilesQueue = '/queue/list-files'
const listFilesResponseQueue = '/queue/list-files-response'

// Função para obter o IP local
function getLocalIP() {
    const interfaces = require('os').networkInterfaces()
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

function sendDiscoveryMessage(callback) {
    const udpClient = dgram.createSocket('udp4')
    const message = Buffer.from('DISCOVER_ACTIVEMQ')

    udpClient.on('listening', () => {
        const address = udpClient.address()
        console.log(`UDP Client listening on ${address.address}:${address.port}`)
        udpClient.addMembership(multicastAddress)
        udpClient.send(message, 0, message.length, udpPort, multicastAddress, (err) => {
            if (err) {
                console.error(`Error sending UDP message: ${err}`)
            } else {
                console.log(`Sent UDP message: "${message}"`)
            }
        })
    })

    udpClient.on('message', (msg, rinfo) => {
        if (rinfo.address !== localIP) {
            console.log(`Received UDP response: "${msg}" from ${rinfo.address}:${rinfo.port}`)
            const match = msg.toString().match(/Server IP: ([\d.]+), ActiveMQ Port: (\d+)/)
            if (match) {
                const [_, ip, port] = match
                callback(ip, parseInt(port, 10))
            } else {
                console.error('Invalid response format')
            }
            udpClient.close()
        }
    })

    udpClient.bind(clientPort, () => {
        console.log(`UDP Client bound to port ${clientPort}`)
    })
}

sendDiscoveryMessage((ip, port) => {
    // Configuração do PostgreSQL
    const sql = postgres({
        user: 'projetosddb_owner',
        host: 'ep-noisy-heart-a57bnu29.us-east-2.aws.neon.tech',
        database: 'projetosddb',
        password: 'Sq5txLc7OPEl',
        port: 5432,
        ssl: 'require',
        connection: {
            options: 'project=ep-noisy-heart-a57bnu29',
        },
    })

    // Configuração do ActiveMQ
    const client = new Stomp(ip, port)

    client.connect(
        () => {
            client.subscribe(downloadQueue, async (body, headers) => {
                const { clientId, fileName } = JSON.parse(body)

                try {
                    const parts = await sql`
          SELECT part_data 
          FROM file_parts fp 
          JOIN files f ON fp.file_id = f.id 
          WHERE f.filename = ${fileName} AND f.client_id = ${clientId} 
          ORDER BY fp.part_number
        `
                    if (parts.length > 0) {
                        parts.forEach((part) => {
                            const message = JSON.stringify({
                                partData: part.part_data.toString('base64'),
                            })
                            client.publish(downloadResponseQueue, message)
                        })
                        client.publish(downloadResponseQueue, JSON.stringify({ complete: true }))
                        console.log(`File ${fileName} download complete for client ${clientId}`)
                    } else {
                        client.publish(
                            downloadResponseQueue,
                            JSON.stringify({ error: 'File not found or not owned by client' })
                        )
                        console.error('File not found or not owned by client')
                    }
                } catch (err) {
                    client.publish(downloadResponseQueue, JSON.stringify({ error: 'Error retrieving file' }))
                    console.error(`Error retrieving file: ${err}`)
                }
            })

            client.subscribe(listFilesQueue, async (body, headers) => {
                const { clientId } = JSON.parse(body)

                try {
                    const files = await sql`
          SELECT filename 
          FROM files 
          WHERE client_id = ${clientId}
        `
                    const fileList = files.map((file) => file.filename)

                    client.publish(
                        listFilesResponseQueue,
                        JSON.stringify({
                            clientId,
                            files: fileList,
                        })
                    )
                } catch (err) {
                    console.error(`Error retrieving file list: ${err}`)
                }
            })
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
        }
    )
})
