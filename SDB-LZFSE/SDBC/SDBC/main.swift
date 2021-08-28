//  main.swift
//  SDBC (SMT LZxyz Compressor):
//
//  Created by Scott D. Bowen on 25/8/21.
//
import Foundation
import DataCompression

var GLOBAL_START = Date()
print("SDBC (SMT LZMA Compressor):")
let dispatchGroup = DispatchGroup()
let MICRO_CHUNK_SIZE = 16 * 1024 * 1024
let THREAD_COUNT = 8

let filename = "~/Testing/enwik/enwik8/enwik8"

let WITH_ALGORITHM = Data.CompressionAlgorithm.lzfse

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
let inputFileURL:  URL = URL(fileURLWithPath: "/Users/sdb/Testing/enwik/enwik8/enwik8")
let outputFileURL: URL = URL(fileURLWithPath: "/Users/sdb/Testing/enwik/enwik8/enwik8.sdb_lzfse_16mb")

let fm = FileManager()
fm.createFile(atPath: outputFileURL.path, contents: Data(), attributes: nil)
let handle = try FileHandle.init(forWritingTo: outputFileURL)

actor Contention {
    
    private var chunks: [Data] = [Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data()]
    
    func updateChunk(_ i: Int, data: Data) {
        chunks[i] = data
    }
    
    func getCRC32(_ i: Int) -> Crc32 {
        return chunks[i].crc32()
    }
    
    func getData(_ i: Int) -> Data {
        return chunks[i]
    }
    
    func getChunkSize(_ i: Int) -> Int {
        return chunks[i].count
    }
}

var contention: Contention = Contention()


var decompressedSize: [Int] = [MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE, MICRO_CHUNK_SIZE]


if let stream = InputStream(url: inputFileURL) {
    dispatchGroup.enter()
    Task {
        var buf = [UInt8](repeating: 0, count: 16 * MICRO_CHUNK_SIZE)

        stream.open()
        while case let amount = stream.read(&buf, maxLength: 16 * MICRO_CHUNK_SIZE), amount > 0 {
            
            if (amount != 16 * MICRO_CHUNK_SIZE) { print(amount) }
            
            let splicedBuffer = (buf.chunked(into: MICRO_CHUNK_SIZE))
            
            await withTaskGroup(of: (Int, Crc32, Int).self) { group in
                
                for i in 0..<16 {
                    group.addTask {
                        
                        var lastBlock: Int = Int.max
                        var truncateArray: Int = MICRO_CHUNK_SIZE
                        if (amount % MICRO_CHUNK_SIZE) > 0 {
                            lastBlock = amount / MICRO_CHUNK_SIZE
                            truncateArray = amount % MICRO_CHUNK_SIZE
                        }

                        var data: Data
                        if (i == lastBlock) {
                            data = Data(splicedBuffer[i].prefix(truncateArray) );
                        } else {
                            data = Data(splicedBuffer[i]);
                        }
                        
                        await contention.updateChunk(i, data: data
                                                .compress(withAlgorithm: WITH_ALGORITHM)!)
                        return await (i, contention.getCRC32(i), amount)
                        // (amount) in return above could be replaced by truncateArray
                    }
                }
                
                for await quaduple in group {
                    // print(quaduple.0, quaduple.1)
                    // BAD: await contention.updateChunk(quaduple.0, data: quaduple.2)
                    decompressedSize[quaduple.0] = quaduple.2
                }
                
                for i in 0..<16 {
                    if (decompressedSize[i] < 16 * MICRO_CHUNK_SIZE) {
                        print()
                        let lastFewBlocksQty = decompressedSize[i] / MICRO_CHUNK_SIZE
                        let lastBlockSizeBytes = decompressedSize[i] % MICRO_CHUNK_SIZE
                        print("Last block is: \(lastBlockSizeBytes) bytes")
                        
                        var i2 = 0
                        while i2 < lastFewBlocksQty {
                            let chunkSize = await UInt32(contention.getChunkSize(i2)   );
                            try? handle.write(contentsOf: Data(from: chunkSize) );
                            try? await handle.write(contentsOf: contention.getData(i2) );
                            try? handle.synchronize()
                            await print(i, i2, chunkSize, contention.getData(i2).count)
                            i2 += 1
                        }
                        let chunkSize = await UInt32(contention.getChunkSize(i2)   );
                        try? handle.write(contentsOf: Data(from: chunkSize) );
                        try? await handle.write(contentsOf: contention.getData(i2) );
                        try? handle.synchronize()
                        await print(i, i2, chunkSize, contention.getData(i2).count)
                        break
                        
                    } else {
                        let chunkSize = await UInt32(contention.getChunkSize(i) );
                        try? handle.write(contentsOf: Data(from: chunkSize) );
                        try? await handle.write(contentsOf: contention.getData(i) );
                        try? handle.synchronize()
                    }
                }
                // print(".", terminator: "")
            }
        }
        dispatchGroup.leave()
        print()
    }
    stream.close()
    // BAD: Hard to diagnose crash: try handle.close()
    dispatchGroup.wait()
}
try handle.close()
benchmarkCode(text: "Compression of: \(filename) (to: .sdb) complete.")

print("Goodbye.")
sleep(3600)
