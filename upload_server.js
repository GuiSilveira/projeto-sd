const dgram = require('dgram')
const postgres = require('postgres')
const Stomp = require('stomp-client')
const crypto = require('crypto')

const multicastAddress = '239.255.255.250'
const udpPort = 41234
const clientPort = 41235
const uploadQueue = '/queue/upload'
const uploadResponseQueue = '/queue/upload-response'
const checkFileQueue = '/queue/check-file'

const encryptionKey = '6e55b04f331955ef56aeaa6e4cf0ffcafeee70761239b2095626e81a2b359bed' // Deve ter 32 caracteres (256 bits)

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

function encryptData(data) {
    const iv = crypto.randomBytes(16)
    const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(encryptionKey), iv)
    let encrypted = cipher.update(data)
    encrypted = Buffer.concat([encrypted, cipher.final()])
    return iv.toString('hex') + ':' + encrypted.toString('hex')
}

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

    const client = new Stomp(ip, port)

    client.connect(
        () => {
            client.subscribe(checkFileQueue, async (body, headers) => {
                const { clientId, fileName } = JSON.parse(body)

                try {
                    const existingFile =
                        await sql`SELECT id FROM files WHERE filename = ${fileName} AND client_id = ${clientId}`
                    const exists = existingFile.length > 0

                    client.publish(
                        uploadResponseQueue,
                        JSON.stringify({
                            clientId,
                            fileName,
                            exists,
                        })
                    )
                } catch (err) {
                    console.error(`Error checking file existence: ${err}`)
                }
            })

            client.subscribe(uploadQueue, async (body, headers) => {
                const { clientId, fileName, partNumber, partData } = JSON.parse(body)
                const chunk = Buffer.from(partData, 'base64')

                try {
                    let fileId

                    try {
                        const result =
                            await sql`INSERT INTO files (filename, client_id) VALUES (${fileName}, ${clientId}) RETURNING id`
                        fileId = result[0].id
                    } catch (insertErr) {
                        if (insertErr.code === '23505') {
                            // Unique violation
                            const existingFile =
                                await sql`SELECT id FROM files WHERE filename = ${fileName} AND client_id = ${clientId}`
                            fileId = existingFile[0].id
                        } else {
                            throw insertErr
                        }
                    }

                    const encryptedChunk = encryptData(chunk)

                    await sql`INSERT INTO file_parts (file_id, part_number, part_data, node_ip) VALUES (${fileId}::int, ${partNumber}::int, ${encryptedChunk}::text, ${localIP}::text)`
                    console.log(`Part ${partNumber} of ${fileName} uploaded successfully by client ${clientId}`)
                } catch (err) {
                    console.error(`Error saving file part: ${err}`)
                }
            })
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
        }
    )
})
