const dgram = require('dgram')
const fs = require('fs')
const path = require('path')
const Stomp = require('stomp-client')
const readline = require('readline')
const { v4: uuidv4 } = require('uuid')

const multicastAddress = '239.255.255.250'
const udpPort = 41234
const clientPort = 41235
const uploadQueue = '/queue/upload'
const uploadResponseQueue = '/queue/upload-response'
const downloadQueue = '/queue/download'
const downloadResponseQueue = '/queue/download-response'
const chunkSize = 1024 // 1 KB
const clientId = uuidv4() // Gera um identificador único para o cliente

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

function uploadFile(ip, port, filePath) {
    const client = new Stomp(ip, port)
    const fileName = path.basename(filePath)

    client.connect(
        () => {
            const fileStream = fs.createReadStream(filePath, { highWaterMark: chunkSize })
            let partNumber = 0

            fileStream.on('data', (chunk) => {
                const message = JSON.stringify({
                    clientId,
                    fileName,
                    partNumber,
                    partData: chunk.toString('base64'),
                })

                client.publish(uploadQueue, message)
                partNumber++
            })

            fileStream.on('end', () => {
                console.log('File upload complete')
                client.disconnect()
            })

            client.subscribe(uploadResponseQueue, (body, headers) => {
                const { clientId, fileName, error } = JSON.parse(body)
                if (error) {
                    console.error(`Error: ${error}`)
                } else {
                    console.log(`File ${fileName} uploaded successfully by client ${clientId}`)
                }
            })
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
        }
    )
}

function downloadFile(ip, port, fileName) {
    const client = new Stomp(ip, port)
    const fileStream = fs.createWriteStream(path.join(__dirname, 'downloaded_' + fileName))

    client.connect(
        () => {
            client.subscribe(downloadResponseQueue, (body, headers) => {
                const { partData, error } = JSON.parse(body)
                if (error) {
                    console.error(error)
                    client.disconnect()
                    return
                }
                const chunk = Buffer.from(partData, 'base64')
                fileStream.write(chunk)
            })

            const message = JSON.stringify({
                clientId,
                fileName,
            })
            client.publish(downloadQueue, message)

            console.log('File download initiated')
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
        }
    )
}

// Interface para escolher entre upload e download
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
})

function askUserChoice(ip, port) {
    rl.question('Você deseja fazer upload ou download de um arquivo? (upload/download/sair): ', (answer) => {
        if (answer.toLowerCase() === 'upload') {
            rl.question('Por favor, insira o caminho do arquivo para upload: ', (filePath) => {
                uploadFile(ip, port, filePath)
                setTimeout(() => askUserChoice(ip, port), 1000) // Permite que a ação de upload termine antes de mostrar o menu novamente
            })
        } else if (answer.toLowerCase() === 'download') {
            rl.question('Por favor, insira o nome do arquivo para download: ', (fileName) => {
                downloadFile(ip, port, fileName)
                setTimeout(() => askUserChoice(ip, port), 1000) // Permite que a ação de download termine antes de mostrar o menu novamente
            })
        } else if (answer.toLowerCase() === 'sair') {
            console.log('Saindo...')
            rl.close()
        } else {
            console.log('Escolha inválida. Por favor, digite "upload", "download" ou "sair".')
            askUserChoice(ip, port)
        }
    })
}

sendDiscoveryMessage((ip, port) => {
    console.log(`Client ID: ${clientId}`)
    askUserChoice(ip, port)
})
