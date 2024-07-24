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
const checkFileQueue = '/queue/check-file'
const downloadQueue = '/queue/download'
const downloadResponseQueue = '/queue/download-response'
const listFilesQueue = '/queue/list-files'
const listFilesResponseQueue = '/queue/list-files-response'
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

function checkFileExistence(ip, port, fileName, callback) {
    const client = new Stomp(ip, port)

    client.connect(
        () => {
            client.subscribe(uploadResponseQueue, (body, headers) => {
                const { clientId, fileName, exists } = JSON.parse(body)
                callback(exists)
                client.disconnect()
            })

            const message = JSON.stringify({
                clientId,
                fileName,
            })
            client.publish(checkFileQueue, message)
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
        }
    )
}

function uploadFile(ip, port, filePath, callback) {
    // Verificação se o arquivo existe antes de tentar o upload
    if (!fs.existsSync(filePath)) {
        console.error(`Error: File ${filePath} does not exist.`)
        callback()
        return
    }

    const client = new Stomp(ip, port)
    const fileName = path.basename(filePath)

    client.connect(
        () => {
            checkFileExistence(ip, port, fileName, (exists) => {
                if (exists) {
                    console.error(`Error: File ${fileName} already exists.`)
                    client.disconnect()
                    callback()
                } else {
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
                        callback()
                    })

                    fileStream.on('error', (err) => {
                        console.error(`Error reading file ${filePath}: ${err}`)
                        client.disconnect()
                        callback()
                    })
                }
            })
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
            callback()
        }
    )
}

function listFiles(ip, port, callback) {
    const client = new Stomp(ip, port)

    client.connect(
        () => {
            client.subscribe(listFilesResponseQueue, (body, headers) => {
                const { clientId, files } = JSON.parse(body)
                callback(files)
                client.disconnect()
            })

            const message = JSON.stringify({
                clientId,
            })
            client.publish(listFilesQueue, message)
        },
        (error) => {
            console.error('Error connecting to ActiveMQ:', error)
            callback([])
        }
    )
}

function downloadFile(ip, port, fileName, callback) {
    const client = new Stomp(ip, port)
    const fileStream = fs.createWriteStream(path.join(__dirname, 'downloaded_' + fileName))

    client.connect(
        () => {
            client.subscribe(downloadResponseQueue, (body, headers) => {
                const { partData, error, complete } = JSON.parse(body)
                if (error) {
                    console.error(error)
                    client.disconnect()
                    callback()
                    return
                }
                if (complete) {
                    console.log('File download complete')
                    fileStream.end()
                    client.disconnect()
                    callback()
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
            callback()
        }
    )
}

// Interface para escolher entre upload e download
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
})

function askUserChoice(ip, port) {
    console.log('Selecione uma opção:')
    console.log('1. Upload de arquivo')
    console.log('2. Download de arquivo')
    console.log('3. Listar arquivos disponíveis para download')
    console.log('4. Sair')
    rl.question('Digite o número da sua escolha: ', (answer) => {
        if (answer === '1') {
            rl.question('Por favor, insira o caminho do arquivo para upload: ', (filePath) => {
                uploadFile(ip, port, filePath, () => {
                    askUserChoice(ip, port)
                })
            })
        } else if (answer === '2') {
            rl.question('Por favor, insira o nome do arquivo para download: ', (fileName) => {
                downloadFile(ip, port, fileName, () => {
                    askUserChoice(ip, port)
                })
            })
        } else if (answer === '3') {
            listFiles(ip, port, (files) => {
                console.log('Arquivos disponíveis para download:', files.join(', '))
                askUserChoice(ip, port)
            })
        } else if (answer === '4') {
            console.log('Saindo...')
            rl.close()
        } else {
            console.log('Escolha inválida. Por favor, digite um número entre 1 e 4.')
            askUserChoice(ip, port)
        }
    })
}

sendDiscoveryMessage((ip, port) => {
    console.log(`Client ID: ${clientId}`)
    askUserChoice(ip, port)
})
