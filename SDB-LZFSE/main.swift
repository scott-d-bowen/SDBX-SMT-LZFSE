//
//  main.swift
//  SDBC
//
//  Created by Scott D. Bowen on 25/8/21.
//

import Foundation
import DataCompression

var GLOBAL_START = Date()
print("SDBC (SMT LZFSE Decompressor):")
let dispatchGroup = DispatchGroup()
let MICRO_CHUNK_SIZE = 1 * 1024 * 1024

extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0 ..< Swift.min($0 + size, count)])
        }
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
var chunks: [Data] = [Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data(),
                      Data(), Data(), Data(), Data(), Data(), Data(), Data(), Data()]

if let stream = InputStream(url: inputFileURL) {
    dispatchGroup.enter()
    Task {
        var buf = [UInt8](repeating: 0, count: 16 * MICRO_CHUNK_SIZE)

        stream.open()
        while case let amount = stream.read(&buf, maxLength: 16 * MICRO_CHUNK_SIZE), amount > 0 {
            
            let splicedBuffer = (buf.chunked(into: MICRO_CHUNK_SIZE))
            
            await withTaskGroup(of: (Int, Crc32, Data).self) { group in
                
                for i in 0..<16 {
                    group.addTask {
                        chunks[i] = Data(splicedBuffer[i]).compress(withAlgorithm: .lzfse)!
                        return (i, chunks[i].crc32(), chunks[i])
                    }
                }
                for await triple in group {
                    // print(triple.0, triple.1)
                    chunks[triple.0] = triple.2
                }
                // let handle = try FileHandle.init(forWritingAtPath: outputFileURL.absoluteString)
                for i in 0..<16 {
                    try? handle.write(contentsOf: chunks[i]);
                    chunks[i] = Data(repeating: 0x00, count: 16)
                }
                print(".", terminator: "")
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
