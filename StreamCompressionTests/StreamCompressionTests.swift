//
//  StreamCompressionTests.swift
//  StreamCompressionTests
//
//  Created by Antoine Marandon on 22/07/2020.
//  Copyright © 2020 eure. All rights reserved.
//

import XCTest
@testable import StreamCompression

class StreamCompressionTests: XCTestCase {
    private var urlsToCleanup = [URL]()

    private func cleanTargetURL() throws -> URL {
        let url = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent(UUID().uuidString, isDirectory: false)
        urlsToCleanup.append(url)
        try? FileManager.default.removeItem(at: url)
        FileManager.default.createFile(atPath: url.path, contents: nil, attributes: nil)
        return url
    }

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        try urlsToCleanup.forEach {
            try FileManager.default.removeItem(at: $0)
        }
    }

    private func write(stream: InputStream, to: URL) throws {
        let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: 4096)
        defer {
            buffer.deallocate()
        }
        let handle = try FileHandle(forWritingTo: to)
        stream.open()
        while true  {
            let count = stream.read(buffer, maxLength: 4096)
            if count > 0 {
                handle.write(Data(bytes: buffer, count: count))
            } else {
                break
            }
        }
        try handle.close()
        stream.close()
    }

    func testToto() throws {
        let gzURL = Bundle(for: type(of: self)).url(forResource: "toto", withExtension: "gz")!
        let stream = try StreamCompression.process(gzURL, mode: .decompress, algorithm: .gzip)
        let destination = try self.cleanTargetURL()
        try self.write(stream: stream, to: destination)
        try XCTAssertEqual("toto\n", String(contentsOfFile: destination.path))
    }

//    func testPerformanceExample() throws {
//        // This is an example of a performance test case.
//        self.measure {
//            // Put the code you want to measure the time of here.
//        }
//    }

}
