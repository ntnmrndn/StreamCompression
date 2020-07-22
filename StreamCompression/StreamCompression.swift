//
//  StreamCompression.swift
//  StreamCompression
//
//  Created by Antoine Marandon on 22/07/2020.
//  Copyright © 2020 eure. All rights reserved.
//

import Foundation
import Compression

private enum GZip {
    fileprivate enum Footer {
        static let size = 8
    }
    fileprivate enum Header {
        enum Error: Swift.Error {
            case badMagic
        }

        private struct Flag: OptionSet {
            let rawValue: UInt8
            static let extra = Flag(rawValue: 1 << 2)
            static let name = Flag(rawValue: 1 << 3)
            static let comment = Flag(rawValue: 1 << 4)
            static let crc = Flag(rawValue: 1 << 1)
        }
        static private let ID1 = UInt8(0x1f)
        static private let ID2 = UInt8(0x8b)
        static private let fixedHeaderSize = 10
        static private let FLGOffset = 3

        /// Returns the size of the header or nil if not enough data was given. Throws on parsing errors
        static func check(_ buffer: UnsafeMutablePointer<UInt8>, lenght: Int) throws -> Int? {
            guard lenght >= fixedHeaderSize else { return nil }
            guard buffer[0] == ID1, buffer[1] == ID2 else { throw Error.badMagic }
            let flag = Flag(rawValue: buffer[FLGOffset])
            var offset: Int = fixedHeaderSize
            if flag.contains(.extra) {
                buffer.withMemoryRebound(to: UInt16.self, capacity: lenght / 2) {
                    offset += Int(UInt16(littleEndian: $0[offset / 2]))
                }
            }
            if flag.contains(.name) {
                while buffer[offset] != 0 && offset > lenght {
                    offset += 1
                }
            }
            if flag.contains(.comment) {
                while buffer[offset] != 0 && offset > lenght {
                    offset += 1
                }
            }
            if flag.contains(.crc) {
                offset += 2
            }
            return offset
        }
    }
}

public enum StreamCompression {
    public enum OperationMode {
        // high level enums are 10.15> :(
        case compress
        case decompress

        fileprivate var mode: compression_stream_operation {
            switch self {
            case .compress:
                return COMPRESSION_STREAM_ENCODE
            case .decompress:
                return COMPRESSION_STREAM_DECODE
            }
        }
    }

    public enum Algorithm {
        case lz4
        case zlib
        case lzma
        case lzfse
        case gzip

        fileprivate var algorithm: compression_algorithm {
            switch self {
            case .lz4:
                return COMPRESSION_LZ4
            case .zlib, .gzip:
                return COMPRESSION_ZLIB
            case .lzma:
                return COMPRESSION_LZMA
            case .lzfse:
                return COMPRESSION_LZFSE
            }
        }
    }

    public static var inputBufferSize = 4096
    public static var outputBufferSize = inputBufferSize * 2 //XXX needs tuning, especially for decompression
    public enum Error: Swift.Error {
        case couldNotCreateStream
        case couldNotInitCompression
        case couldNotProcessData
    }

    //XXX one of the architectural limitation is that we wait for input starvation to refill the input buffer, leading to sometime smaller output buffers. We should try to provide as much data as possible, all the time.
    private class Operation: InputStream, StreamDelegate {
        private let mode: OperationMode
        private let algorithm: Algorithm
        private var status = InputStream.Status.notOpen
        private var error: Swift.Error?
        private let inputStream: InputStream
        private let outputBuffer: UnsafeMutablePointer<UInt8> = .allocate(capacity: outputBufferSize)
        private let inputBuffer: UnsafeMutablePointer<UInt8> = .allocate(capacity: inputBufferSize)
        let compressionStream = UnsafeMutablePointer<compression_stream>.allocate(capacity: 1)
        private var currentReadIndex: Int = 0
        private var availableBytes: Int {
            return compressionStream.pointee.dst_ptr - outputBuffer.advanced(by: currentReadIndex)
        }
        deinit {
            close()
        }
        private var nextFlag: Int32 = 0
        private var finishedInputBuffer = true
        private var runLoop: RunLoop?
        private var runLoopMode: RunLoop.Mode?
        private var hasProcessedGzipHeader: Bool = false


        private func fillOutputBuffer() {
            switch compression_stream_process(compressionStream, nextFlag) {
            case COMPRESSION_STATUS_OK:
                break
            case COMPRESSION_STATUS_END:
                finishedInputBuffer = true
            case COMPRESSION_STATUS_ERROR:
                status = .error
            default:
                fatalError("Unkwonwn result")
            }
            nextFlag = Int32(COMPRESSION_STREAM_FINALIZE.rawValue)
        }

        /// Fills input buffer, reset output
        private func nextInput() {
            var availableSize = inputStream.read(inputBuffer, maxLength: inputBufferSize)
            guard availableSize > 0 else {
                status = .error
                error = inputStream.streamError
                return
            }

            if self.algorithm == .gzip && self.hasProcessedGzipHeader == false {
                if let headerSize = try? GZip.Header.check(inputBuffer, lenght: availableSize) {
                    compressionStream.pointee.src_ptr = UnsafePointer(inputBuffer.advanced(by: headerSize))
                    availableSize -= headerSize
                    self.hasProcessedGzipHeader = true
                } else {
                    status = .error
                    error = inputStream.streamError
                    return
                }
            } else {
                compressionStream.pointee.src_ptr = UnsafePointer(inputBuffer)
            }
            compressionStream.pointee.src_size = availableSize
        }

        private var hasFinishedReadingOutput: Bool {
            outputBuffer.advanced(by: currentReadIndex) == compressionStream.pointee.dst_ptr
        }

        private func process() {
            guard status != .closed && status != .atEnd else { return }
            if hasFinishedReadingOutput { /// Reset target buffer if everything was read
                compressionStream.pointee.dst_ptr = outputBuffer
                compressionStream.pointee.dst_size = outputBufferSize
                currentReadIndex = 0
            }
            if finishedInputBuffer { // compression has read all available data
                nextInput()
                nextFlag = 0
            }
            fillOutputBuffer()
        }

        fileprivate init(_ stream: InputStream, mode: OperationMode, algorithm: Algorithm) throws {
            status = InputStream.Status.notOpen
            self.mode = mode
            self.algorithm = algorithm
            inputStream = stream
            guard compression_stream_init(compressionStream, mode.mode, algorithm.algorithm) == COMPRESSION_STATUS_OK else {
                throw Error.couldNotInitCompression
            }
            super.init(data: Data())
            compressionStream.pointee.dst_ptr = outputBuffer
            compressionStream.pointee.dst_size = outputBufferSize
            self.inputStream.delegate = self
        }

        @objc dynamic public func stream(_ aStream: Stream, handle eventCode: Stream.Event) {
            /// Passive implementation: we do not process the data actively but wait for a read to ask for some before processing it.
            switch eventCode {
            case .endEncountered:
                return //XXX
            case .errorOccurred,
                 .hasBytesAvailable,
                 .hasSpaceAvailable,
                 .openCompleted:
                guard let runLoop = runLoop, let runLoopMode = runLoopMode else { return }
                runLoop.perform(#selector(Self.notify(eventCode:)), target: self, argument: eventCode, order: 1, modes: [runLoopMode])
            default:
                assertionFailure("Unknow stream event occured") //XXX
                return
            }
        }

        @objc private func notify(eventCode: Stream.Event) {
            delegate?.stream?(self, handle: eventCode)
        }

        override func read(_ buffer: UnsafeMutablePointer<UInt8>, maxLength len: Int) -> Int {
            if availableBytes == 0 {
                process()
            }
            let readSize = min(availableBytes - currentReadIndex, len)
            memcpy(buffer, self.outputBuffer.advanced(by: currentReadIndex), readSize)
            currentReadIndex += readSize
            return readSize
        }

        override func getBuffer(_ buffer: UnsafeMutablePointer<UnsafeMutablePointer<UInt8>?>, length len: UnsafeMutablePointer<Int>) -> Bool {
            return false
        }

        override var hasBytesAvailable: Bool {
            return availableBytes > 0
        }

        override var streamStatus: Stream.Status {
            status
        }

        override var streamError: Swift.Error? {
            error
        }

        private weak var _delegate: StreamDelegate?
        override var delegate: StreamDelegate? {
            set {
                _delegate = newValue
            }
            get {
                _delegate
            }
        }

        override func open() {
            guard status == .notOpen else {
                fatalError()
            }
            inputStream.open()
            status = .open
            process()
        }

        override func close() {
            guard status != .closed else { return }
            status = .closed
            outputBuffer.deallocate()
            inputBuffer.deallocate()
            compressionStream.deallocate()
            guard compression_stream_destroy(compressionStream) == COMPRESSION_STATUS_OK else {
                preconditionFailure()
            }
            self.runLoop = nil
            self.delegate = nil
            self.inputStream.close()
        }

        override func schedule(in aRunLoop: RunLoop, forMode mode: RunLoop.Mode) {
            inputStream.schedule(in: aRunLoop, forMode: mode)
            self.runLoop = aRunLoop
        }

        override func property(forKey key: Stream.PropertyKey) -> Any? {
            super.property(forKey: key)
        }

        override func setProperty(_ property: Any?, forKey key: Stream.PropertyKey) -> Bool {
            super.setProperty(property, forKey: key)
        }
    }

    public static func process(_ stream: InputStream, mode: OperationMode, algorithm: Algorithm) throws -> InputStream {
        return try Operation(stream, mode: mode, algorithm: algorithm)
    }

    public static func process(_ url: URL, mode: OperationMode, algorithm: Algorithm) throws -> InputStream {
        guard let inputStream = InputStream(fileAtPath: url.path) else {
            throw Error.couldNotCreateStream
        }
        return try process(inputStream, mode: mode, algorithm: algorithm)
    }
}
