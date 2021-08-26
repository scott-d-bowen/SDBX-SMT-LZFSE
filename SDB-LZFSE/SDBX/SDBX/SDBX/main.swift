//
//  main.swift
//  SDBX
//
//  Created by Scott D. Bowen on 25/8/21.
//

import Foundation
import DataCompression

var GLOBAL_START = Date()
print("SDBX (LZFSE Decompressor):")
let dispatchGroup = DispatchGroup()
let MICRO_CHUNK_SIZE = 1 * 1024 * 1024

enum InputStreamError: Error {
    case invalidData
}
extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0 ..< Swift.min($0 + size, count)])
        }
    }
}
extension Data {
    
    init<T>(from value: T) {
        self = Swift.withUnsafeBytes(of: value) { Data($0) }
    }
    
    func to<T>(type: T.Type) -> T? where T: ExpressibleByIntegerLiteral {
        var value: T = 0 // as! T
        guard count >= MemoryLayout.size(ofValue: value) else { return nil }
        _ = Swift.withUnsafeMutableBytes(of: &value, { copyBytes(to: $0)} )
        return value
    }
}
func benchmarkCode(text: String) {
    let value = -GLOBAL_START.timeIntervalSinceNow
    print("\(Int64(value * 1_000_000)) nanoseconds \(text)")
    GLOBAL_START = Date()
}

// Compression Algorithm Test:

let inputFileURL:  URL = URL(fileURLWithPath: "/Users/sdb/Testing/Xcode13-beta5.sdb")
let outputFileURL: URL = URL(fileURLWithPath: "/Users/sdb/Testing/Xcode13-beta5.tar.extracted")
let fm = FileManager()
fm.createFile(atPath: outputFileURL.path, contents: Data(), attributes: nil)
let handle = try FileHandle.init(forWritingTo: outputFileURL)
var chunk: Data = Data()

if let stream = InputStream(url: inputFileURL) {
    dispatchGroup.enter()
    
    var bufA = [UInt8](repeating: 0, count: 4)
    var bufB = [UInt8](repeating: 0, count: 4 * MICRO_CHUNK_SIZE)
    
    stream.open()
    var chunkSizeA = stream.read(&bufA, maxLength: 4)
    guard (chunkSizeA == 4) else {
        print(" * Error decompressig data input stream.")
        throw InputStreamError.invalidData
    }
    var chunkSizeB = Data(bufA).to(type: UInt32.self)
    
    var iteration = 1
    while case let amount = stream.read(&bufB, maxLength: Int(chunkSizeB!)), amount > 0 {
        
        bufB = Array(bufB.prefix(amount))
        print(iteration, amount, bufB.count, terminator: " ")
        chunk = Data(bufB).decompress(withAlgorithm: .lzfse)! // ?? Data()
        print(chunk.crc32(), chunk.count)

        try? handle.write(contentsOf: chunk);

        chunkSizeA = stream.read(&bufA, maxLength: 4)
        chunkSizeB = Data(bufA).to(type: UInt32.self)
        
        bufB = [UInt8](repeating: 0, count: 4 * MICRO_CHUNK_SIZE)
        
        iteration += 1
    }
    dispatchGroup.leave()
    stream.close()
    try handle.close()
    dispatchGroup.wait()
}
benchmarkCode(text: "Decompression of: Xcode13-beta5.tar.extracted (from: .sdb) complete.")

print("Goodbye.")
sleep(3600)
