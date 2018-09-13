//
//  MQTT.swift
//  MQTTExample
//
//  Created by Cesar A. Guayara L. on 20/06/2017.
//  Copyright © 2017 INGCAG. All rights reserved.
//

import Foundation
//import "AsyncSocket.h"
//import CocoaAsyncSocket
//import SwiftyTimer

/**
 * QOS
 */
@objc public enum MQTTQOS: UInt8 {
    case qos0 = 0
    case qos1
    case qos2
}

/**
 * Connection State
 */
public enum MQTTConnState: UInt8 {
    case initial = 0
    case connecting
    case connected
    case disconnected
}

/**
 * Conn Ack
 */
@objc public enum MQTTConnAck: UInt8 {
    case accept  = 0
    case unacceptableProtocolVersion
    case identifierRejected
    case serverUnavailable
    case badUsernameOrPassword
    case notAuthorized
    case reserved
}

/**
 * asyncsocket read tag
 */
fileprivate enum MQTTReadTag: Int {
    case header = 0
    case length
    case payload
}

/**
 * MQTT Delegate
 */
@objc public protocol MQTTDelegate {
    /// MQTT connected with server
    func mqtt(_ mqtt: MQTT, didConnect host: String, port: Int)
    func mqtt(_ mqtt: MQTT, didConnectAck ack: MQTTConnAck)
    func mqtt(_ mqtt: MQTT, didPublishMessage message: MQTTMessage, id: UInt16)
    func mqtt(_ mqtt: MQTT, didPublishAck id: UInt16)
    func mqtt(_ mqtt: MQTT, didReceiveMessage message: MQTTMessage, id: UInt16 )
    func mqtt(_ mqtt: MQTT, didSubscribeTopic topic: String)
    func mqtt(_ mqtt: MQTT, didUnsubscribeTopic topic: String)
    func mqttDidPing(_ mqtt: MQTT)
    func mqttDidReceivePong(_ mqtt: MQTT)
    func mqttDidDisconnect(_ mqtt: MQTT, withError err: Error?)
    @objc optional func mqtt(_ mqtt: MQTT, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Void)
    @objc optional func mqtt(_ mqtt: MQTT, didPublishComplete id: UInt16)
}

/**
 * Blueprint of the MQTT client
 */
protocol MQTTClient {
    var host: String { get set }
    var port: UInt16 { get set }
    var clientID: String { get }
    var username: String? {get set}
    var password: String? {get set}
    var cleanSession: Bool {get set}
    var keepAlive: UInt16 {get set}
    var willMessage: MQTTWill? {get set}
    
    func connect() -> Bool
    func disconnect()
    func ping()
    
    func subscribe(_ topic: String, qos: MQTTQOS) -> UInt16
    func unsubscribe(_ topic: String) -> UInt16
    func publish(_ topic: String, withString string: String, qos: MQTTQOS, retained: Bool, dup: Bool) -> UInt16
    func publish(_ message: MQTTMessage) -> UInt16
    
}

/**
 * MQTT Reader Delegate
 */
protocol MQTTReaderDelegate {
    func didReceiveConnAck(_ reader: MQTTReader, connack: UInt8)
    func didReceivePublish(_ reader: MQTTReader, message: MQTTMessage, id: UInt16)
    func didReceivePubAck(_ reader: MQTTReader, msgid: UInt16)
    func didReceivePubRec(_ reader: MQTTReader, msgid: UInt16)
    func didReceivePubRel(_ reader: MQTTReader, msgid: UInt16)
    func didReceivePubComp(_ reader: MQTTReader, msgid: UInt16)
    func didReceiveSubAck(_ reader: MQTTReader, msgid: UInt16)
    func didReceiveUnsubAck(_ reader: MQTTReader, msgid: UInt16)
    func didReceivePong(_ reader: MQTTReader)
}

extension Int {
    var MB: Int {
        return self * 1024 * 1024
    }
}

/**
 * Main MQTT Class
 *
 * Notice: GCDAsyncSocket need delegate to extend NSObject
 */
open class MQTT: NSObject, MQTTClient, MQTTFrameBufferProtocol {
    open var host: String
    open var port: UInt16
    open var clientID: String
    //open var host = "m23.cloudmqtt.com"
    //open var port: UInt16 = 1883
    //open var clientID: String
    
    //open var host = "localhost"
    //open var port: UInt16 = 1883
    //open var clientID: String
    open var username: String?
    open var password: String?
    open var secureMQTT = false
    open var cleanSession = true
    open var willMessage: MQTTWill?
    open weak var delegate: MQTTDelegate?
    open var backgroundOnSocket = false
    open var connState = MQTTConnState.initial
    open var dispatchQueue = DispatchQueue.main
    
    // flow control
    var buffer = MQTTFrameBuffer()
    
    
    // heart beat
    open var keepAlive: UInt16 = 60
    fileprivate var aliveTimer: Timer?
    
    // auto reconnect
    open var autoReconnect = false
    open var autoReconnectTimeInterval: UInt16 = 20
    fileprivate var autoReconnTimer: Timer?
    fileprivate var disconnectExpectedly = false
    
    // log
    open var logLevel: MQTTLoggerLevel {
        get {
            return MQTTLogger.logger.minLevel
        }
        set {
            MQTTLogger.logger.minLevel = newValue
        }
    }
    
    // ssl
    open var enableSSL = false
    open var sslSettings: [String: NSObject]?
    open var allowUntrustCACertificate = false
    
    // subscribed topics. (dictionary structure -> [msgid: [topicString: QoS]])
    open var subscriptions: [UInt16: [String: MQTTQOS]] = [:]
    var subscriptionsWaitingAck: [UInt16: [String: MQTTQOS]] = [:]
    var unsubscriptionsWaitingAck: [UInt16: [String: MQTTQOS]] = [:]
    
    // global message id
    var gmid: UInt16 = 1
    var socket = GCDAsyncSocket()
    var reader: MQTTReader?
    
    
    // MARK: init
    //public init(clientID: String, host: String = "localhost", port: UInt16 = 1883) {
    public init(clientID: String, host: String, port: UInt16) {
        self.clientID = clientID
        self.host = host
        self.port = port
        super.init()
        buffer.delegate = self
    }
    
    deinit {
        aliveTimer?.invalidate()
        autoReconnTimer?.invalidate()
        
        socket.delegate = nil
        socket.disconnect()
    }
    
    public func buffer(_ buffer: MQTTFrameBuffer, sendPublishFrame frame: MQTTFramePublish) {
        send(frame, tag: Int(frame.msgid!))
    }
    
    fileprivate func send(_ frame: MQTTFrame, tag: Int = 0) {
        let data = frame.data()
        socket.write(Data(bytes: data, count: data.count), withTimeout: -1, tag: tag)
    }
    
    fileprivate func sendConnectFrame() {
        let frame = MQTTFrameConnect(client: self)
        send(frame)
        reader!.start()
        delegate?.mqtt(self, didConnect: host, port: Int(port))
    }
    
    fileprivate func nextMessageID() -> UInt16 {
        if gmid == UInt16.max {
            gmid = 0
        }
        gmid += 1
        return gmid
    }
    
    fileprivate func puback(_ type: MQTTFrameType, msgid: UInt16) {
        var descr: String?
        switch type {
        case .puback:
            descr = "PUBACK"
        case .pubrec:
            descr = "PUBREC"
        case .pubrel:
            descr = "PUBREL"
        case .pubcomp:
            descr = "PUBCOMP"
        default: break
        }
        
        if descr != nil {
            printDebug("Send \(descr!), msgid: \(msgid)")
        }
        
        send(MQTTFramePubAck(type: type, msgid: msgid))
    }
    
    @discardableResult
    open func connect() -> Bool {
        socket.setDelegate(self, delegateQueue: dispatchQueue)
        reader = MQTTReader(socket: socket, delegate: self)
        do {
            try socket.connect(toHost: self.host, onPort: self.port)
            connState = .connecting
            return true
        } catch let error as NSError {
            printError("socket connect error: \(error.description)")
            return false
        }
    }
    
    /// Only can be called from outside. If you want to disconnect from inside framwork, call internal_disconnect()
    /// disconnect expectedly
    open func disconnect() {
        disconnectExpectedly = true
        internal_disconnect()
    }
    
    /// disconnect unexpectedly
    open func internal_disconnect() {
        send(MQTTFrame(type: MQTTFrameType.disconnect), tag: -0xE0)
        socket.disconnect()
    }
    
    open func ping() {
        printDebug("ping")
        send(MQTTFrame(type: MQTTFrameType.pingreq), tag: -0xC0)
        self.delegate?.mqttDidPing(self)
    }
    
    @discardableResult
    open func publish(_ topic: String, withString string: String, qos: MQTTQOS = .qos1, retained: Bool = false, dup: Bool = false) -> UInt16 {
        let message = MQTTMessage(topic: topic, string: string, qos: qos, retained: retained, dup: dup)
        return publish(message)
    }
    
    @discardableResult
    open func publish(_ message: MQTTMessage) -> UInt16 {
        let msgid: UInt16 = nextMessageID()
        let frame = MQTTFramePublish(msgid: msgid, topic: message.topic, payload: message.payload)
        frame.qos = message.qos.rawValue
        frame.retained = message.retained
        frame.dup = message.dup
                send(frame, tag: Int(msgid))
        _ = buffer.add(frame)
        
        
        
        if message.qos != MQTTQOS.qos0 {
            
        }
        
        
        delegate?.mqtt(self, didPublishMessage: message, id: msgid)
        
        return msgid
    }
    
    @discardableResult
    open func subscribe(_ topic: String, qos: MQTTQOS = .qos1) -> UInt16 {
        let msgid = nextMessageID()
        let frame = MQTTFrameSubscribe(msgid: msgid, topic: topic, reqos: qos.rawValue)
        send(frame, tag: Int(msgid))
        subscriptionsWaitingAck[msgid] = [topic:qos]
        return msgid
    }
    
    @discardableResult
    open func unsubscribe(_ topic: String) -> UInt16 {
        let msgid = nextMessageID()
        let frame = MQTTFrameUnsubscribe(msgid: msgid, topic: topic)
        unsubscriptionsWaitingAck[msgid] = [topic:MQTTQOS.qos0]
        send(frame, tag: Int(msgid))
        return msgid
    }
}

// MARK: - GCDAsyncSocketDelegate
extension MQTT: GCDAsyncSocketDelegate {
    public func socket(_ sock: GCDAsyncSocket, didConnectToHost host: String, port: UInt16) {
        printDebug("connected to \(host) : \(port)")
        
        #if TARGET_OS_IPHONE
            if backgroundOnSocket {
                sock.performBlock { sock.enableBackgroundingOnSocket() }
            }
        #endif
        
        if enableSSL {
            if sslSettings == nil {
                if allowUntrustCACertificate {
                    sock.startTLS([GCDAsyncSocketManuallyEvaluateTrust: true as NSObject]) }
                else {
                    sock.startTLS(nil)
                }
            } else {
                sslSettings![GCDAsyncSocketManuallyEvaluateTrust as String] = NSNumber(value: true)
                sock.startTLS(sslSettings!)
            }
        } else {
            sendConnectFrame()
        }
    }
    
    public func socket(_ sock: GCDAsyncSocket, didReceive trust: SecTrust, completionHandler: @escaping (Bool) -> Swift.Void) {
        printDebug("didReceiveTrust")
        
        delegate?.mqtt!(self, didReceive: trust, completionHandler: completionHandler)
    }
    
    public func socketDidSecure(_ sock: GCDAsyncSocket) {
        printDebug("socketDidSecure")
        sendConnectFrame()
    }
    
    public func socket(_ sock: GCDAsyncSocket, didWriteDataWithTag tag: Int) {
        printDebug("Socket write message with tag: \(tag)")
    }
    
    public func socket(_ sock: GCDAsyncSocket, didRead data: Data, withTag tag: Int) {
        let etag = MQTTReadTag(rawValue: tag)!
        var bytes = [UInt8]([0])
        switch etag {
        case MQTTReadTag.header:
            data.copyBytes(to: &bytes, count: 1)
            reader!.headerReady(bytes[0])
        case MQTTReadTag.length:
            data.copyBytes(to: &bytes, count: 1)
            reader!.lengthReady(bytes[0])
        case MQTTReadTag.payload:
            reader!.payloadReady(data)
        }
    }
    
    public func socketDidDisconnect(_ sock: GCDAsyncSocket, withError err: Error?) {
        socket.delegate = nil
        connState = .disconnected
        delegate?.mqttDidDisconnect(self, withError: err)
        
        autoReconnTimer?.invalidate()
        if !disconnectExpectedly && autoReconnect && autoReconnectTimeInterval > 0 {
            autoReconnTimer = Timer.every(Double(autoReconnectTimeInterval).seconds, { [weak self] (timer: Timer) in
                printDebug("try reconnect")
                self?.connect()
            })
        }
    }
}

// MARK: - MQTTReaderDelegate
extension MQTT: MQTTReaderDelegate {
    func didReceiveConnAck(_ reader: MQTTReader, connack: UInt8) {
        printDebug("CONNACK Received: \(connack)")
        
        let ack: MQTTConnAck
        switch connack {
        case 0:
            ack = .accept
            connState = .connected
        case 1...5:
            ack = MQTTConnAck(rawValue: connack)!
            internal_disconnect()
        case _ where connack > 5:
            ack = .reserved
            internal_disconnect()
        default:
            internal_disconnect()
            return
        }
        
        delegate?.mqtt(self, didConnectAck: ack)
        
        // auto reconnect
        if ack == MQTTConnAck.accept {
            autoReconnTimer?.invalidate()
            disconnectExpectedly = false
        }
        
        // keep alive
        if ack == MQTTConnAck.accept && keepAlive > 0 {
            aliveTimer?.invalidate()
            aliveTimer = Timer.every(Double(keepAlive).seconds) { [weak self] (timer: Timer) in
                if self?.connState == .connected {
                    self?.ping()
                } else {
                    timer.invalidate()
                }
            }
        }
    }
    
    func didReceivePublish(_ reader: MQTTReader, message: MQTTMessage, id: UInt16) {
        printDebug("PUBLISH Received from \(message.topic)")
        
        delegate?.mqtt(self, didReceiveMessage: message, id: id)
        if message.qos == MQTTQOS.qos1 {
            puback(MQTTFrameType.puback, msgid: id)
        } else if message.qos == MQTTQOS.qos2 {
            puback(MQTTFrameType.pubrec, msgid: id)
        }
    }
    
    func didReceivePubAck(_ reader: MQTTReader, msgid: UInt16) {
        printDebug("PUBACK Received: \(msgid)")
        
        buffer.sendSuccess(withMsgid: msgid)
        delegate?.mqtt(self, didPublishAck: msgid)
    }
    
    func didReceivePubRec(_ reader: MQTTReader, msgid: UInt16) {
        printDebug("PUBREC Received: \(msgid)")
        
        puback(MQTTFrameType.pubrel, msgid: msgid)
    }
    
    func didReceivePubRel(_ reader: MQTTReader, msgid: UInt16) {
        printDebug("PUBREL Received: \(msgid)")
        
        puback(MQTTFrameType.pubcomp, msgid: msgid)
    }
    
    func didReceivePubComp(_ reader: MQTTReader, msgid: UInt16) {
        printDebug("PUBCOMP Received: \(msgid)")
        
        buffer.sendSuccess(withMsgid: msgid)
        delegate?.mqtt?(self, didPublishComplete: msgid)
    }
    
    func didReceiveSubAck(_ reader: MQTTReader, msgid: UInt16) {
        printDebug("SUBACK Received: \(msgid)")
        
        if let topicDict = subscriptionsWaitingAck.removeValue(forKey: msgid) {
            let topic = topicDict.first!.key
            
            // remove subscription with same topic
            for (key, value) in subscriptions {
                if value.first!.key == topic {
                    subscriptions.removeValue(forKey: key)
                }
            }
            
            subscriptions[msgid] = topicDict
            delegate?.mqtt(self, didSubscribeTopic: topic)
        } else {
            printWarning("UNEXPECT SUBACK Received: \(msgid)")
        }
    }
    
    func didReceiveUnsubAck(_ reader: MQTTReader, msgid: UInt16) {
        printDebug("UNSUBACK Received: \(msgid)")
        
        
        if let topicDict = unsubscriptionsWaitingAck.removeValue(forKey: msgid) {
            let topic = topicDict.first!.key
            
            for (key, value) in subscriptions {
                if value.first!.key == topic {
                    subscriptions.removeValue(forKey: key)
                }
            }
            
            delegate?.mqtt(self, didUnsubscribeTopic: topic)
        } else {
            printWarning("UNEXPECT UNSUBACK Received: \(msgid)")
        }
    }
    
    func didReceivePong(_ reader: MQTTReader) {
        printDebug("PONG Received")
        
        delegate?.mqttDidReceivePong(self)
    }
}

class MQTTReader {
    private var socket: GCDAsyncSocket
    private var header: UInt8 = 0
    private var length: UInt = 0
    private var data: [UInt8] = []
    private var multiply = 1
    private var delegate: MQTTReaderDelegate
    private var timeout = 30000
    
    init(socket: GCDAsyncSocket, delegate: MQTTReaderDelegate) {
        self.socket = socket
        self.delegate = delegate
    }
    
    func start() {
        readHeader()
    }
    
    func headerReady(_ header: UInt8) {
        printDebug("reader header ready: \(header) ")
        
        self.header = header
        readLength()
    }
    
    func lengthReady(_ byte: UInt8) {
        length += (UInt)((Int)(byte & 127) * multiply)
        // done
        if byte & 0x80 == 0 {
            if length == 0 {
                frameReady()
            } else {
                readPayload()
            }
            // more
        } else {
            multiply *= 128
            readLength()
        }
    }
    
    func payloadReady(_ data: Data) {
        self.data = [UInt8](repeating: 0, count: data.count)
        data.copyBytes(to: &(self.data), count: data.count)
        frameReady()
    }
    
    private func readHeader() {
        reset()
        socket.readData(toLength: 1, withTimeout: -1, tag: MQTTReadTag.header.rawValue)
    }
    
    private func readLength() {
        socket.readData(toLength: 1, withTimeout: TimeInterval(timeout), tag: MQTTReadTag.length.rawValue)
    }
    
    private func readPayload() {
        socket.readData(toLength: length, withTimeout: TimeInterval(timeout), tag: MQTTReadTag.payload.rawValue)
    }
    
    private func frameReady() {
        // handle frame
        let frameType = MQTTFrameType(rawValue: UInt8(header & 0xF0))!
        switch frameType {
        case .connack:
            delegate.didReceiveConnAck(self, connack: data[1])
        case .publish:
            let (msgid, message) = unpackPublish()
            if message != nil {
                delegate.didReceivePublish(self, message: message!, id: msgid)
            }
        case .puback:
            delegate.didReceivePubAck(self, msgid: msgid(data))
        case .pubrec:
            delegate.didReceivePubRec(self, msgid: msgid(data))
        case .pubrel:
            delegate.didReceivePubRel(self, msgid: msgid(data))
        case .pubcomp:
            delegate.didReceivePubComp(self, msgid: msgid(data))
        case .suback:
            delegate.didReceiveSubAck(self, msgid: msgid(data))
        case .unsuback:
            delegate.didReceiveUnsubAck(self, msgid: msgid(data))
        case .pingresp:
            delegate.didReceivePong(self)
        default:
            break
        }
        
        readHeader()
    }
    
    private func unpackPublish() -> (UInt16, MQTTMessage?) {
        let frame = MQTTFramePublish(header: header, data: data)
        frame.unpack()
        // if unpack fail
        if frame.msgid == nil {
            return (0, nil)
        }
        let msgid = frame.msgid!
        let qos = MQTTQOS(rawValue: frame.qos)!
        let message = MQTTMessage(topic: frame.topic!, payload: frame.payload, qos: qos, retained: frame.retained, dup: frame.dup)
        return (msgid, message)
    }
    
    private func msgid(_ bytes: [UInt8]) -> UInt16 {
        if bytes.count < 2 { return 0 }
        return UInt16(bytes[0]) << 8 + UInt16(bytes[1])
    }
    
    private func reset() {
        length = 0
        multiply = 1
        header = 0
        data = []
    }
}



/// MARK - Logger

public enum MQTTLoggerLevel {
    case debug, warning, error, off
}

public class MQTTLogger: NSObject {
    
    // Singleton
    static let logger = MQTTLogger()
    private override init() {}
    
    // min level
    public var minLevel: MQTTLoggerLevel = .warning
    
    // logs
    func log(level: MQTTLoggerLevel, message: String) {
        guard level.hashValue >= minLevel.hashValue else { return }
        print("MQTT(\(level)): \(message)")
    }
    
    func debug(_ message: String) {
        log(level: .debug, message: message)
    }
    
    func warning(_ message: String) {
        log(level: .warning, message: message)
    }
    
    func error(_ message: String) {
        log(level: .error, message: message)
    }
    
}

// Convenience functions
public func printDebug(_ message: String) {
    MQTTLogger.logger.debug(message)
}

public func printWarning(_ message: String) {
    MQTTLogger.logger.warning(message)
}

public func printError(_ message: String) {
    MQTTLogger.logger.error(message)
}
