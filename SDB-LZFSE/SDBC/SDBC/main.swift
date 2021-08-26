//
//  main.swift
//  main
//
//  Created by Scott D. Bowen on 25/8/21.
//

import Foundation
import DataCompression

var GLOBAL_START = Date()
print("SDBC (SMT LZFSE Compressor):")
let dispatchGroup = DispatchGroup()
let MICRO_CHUNK_SIZE = 1 * 1024 * 1024

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
        var value: T = 0
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
let inputFileURL:  URL = URL(fileURLWithPath: "/Users/sdb/Testing/Xcode13-beta5.tar")
let outputFileURL: URL = URL(fileURLWithPath: "/Users/sdb/Testing/Xcode13-beta5.sdb")
let fm = FileManager()
fm.createFile(atPath: outputFileURL.path, contents: Data(), attributes: nil)
let handle = try FileHandle.init(forWritingTo: outputFileURL)

actor Contention {
    private static var chunks: [Data] = [Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data()]
    
    static func updateChunk(_ i: Int, data: Data) {
        chunks[i] = data
    }
    
    static func getCRC32(_ i: Int) -> Crc32 {
        return chunks[i].crc32()
    }
    
    static func getData(_ i: Int) -> Data {
        return chunks[i]
    }
    
    static func getChunkSize(_ i: Int) -> Int {
        return chunks[i].count
    }
}


var decompressedSize: [Int] = [MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE]


if let stream = InputStream(url: inputFileURL) {
    dispatchGroup.enter()
    Task {
        var buf = [UInt8](repeating: 0, count: 16 * MICRO_CHUNK_SIZE)

        stream.open()
        while case let amount = stream.read(&buf, maxLength: 16 * MICRO_CHUNK_SIZE), amount > 0 {
            print(amount)
            
            let splicedBuffer = (buf.chunked(into: MICRO_CHUNK_SIZE))
            
            await withTaskGroup(of: (Int, Crc32, Data, Int).self) { group in
                
                for i in 0..<16 {
                    group.addTask {
                        Contention.updateChunk(i, data: Data(splicedBuffer[i])
                                                .compress(withAlgorithm: .lzfse)!)
                        return (i, Contention.getCRC32(i), Contention.getData(i), amount)
                    }
                }
                for await quaduple in group {
                    // print(quaduple.0, quaduple.1)
                    Contention.updateChunk(quaduple.0, data: quaduple.2)
                    decompressedSize[quaduple.0] = quaduple.3
                }
                // let handle = try FileHandle.init(forWritingAtPath: outputFileURL.absoluteString)
                for i in 0..<16 {
                    if (decompressedSize[i] < 16 * MICRO_CHUNK_SIZE) {
                        print()
                        let lastFewBlocksQty = decompressedSize[i] / MICRO_CHUNK_SIZE
                        let lastBlockSizeBytes = decompressedSize[i] % MICRO_CHUNK_SIZE
                        print("Last block is: \(lastBlockSizeBytes) bytes")
                        for _ in 0..<lastFewBlocksQty {
                            let chunkSize = UInt32(Contention.getChunkSize(i)   );
                            try? handle.write(contentsOf: Data(from: chunkSize) );
                            try? handle.write(contentsOf: Contention.getData(i) );
                        }
                        try? handle.write(contentsOf: Data(from: lastBlockSizeBytes) );
                        try? handle.write(contentsOf: Contention.getData(i) );
                        break
                    } else {
                        let chunkSize = UInt32(Contention.getChunkSize(i)   );
                        try? handle.write(contentsOf: Data(from: chunkSize) );
                        try? handle.write(contentsOf: Contention.getData(i) );
                        // Dumb Idea?: Contention.chunks[i] = Data(repeating: 0x00, count: 16)
                    }
                }
                // print(".", terminator: "")
            }
        }
        dispatchGroup.leave()
        print()
    }
    stream.close()
    dispatchGroup.wait()
}
benchmarkCode(text: "Compression of: Xcode13-beta5.tar (to: .sdb) complete.")


print("Goodbye.")
sleep(3600)
