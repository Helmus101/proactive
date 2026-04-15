import Foundation
import Vision
import AppKit

struct OCRResult: Encodable {
    let text: String
    let lines: [String]
    let confidence: Double
}

struct OCRLine {
    let text: String
    let confidence: Double
    let x: Double
    let y: Double
}

enum OCRFailure: Error {
    case invalidArguments
    case imageLoadFailed
}

func runOCR(imagePath: String, languageHint: String?) throws -> OCRResult {
    let url = URL(fileURLWithPath: imagePath)
    guard let image = NSImage(contentsOf: url) else {
        throw OCRFailure.imageLoadFailed
    }

    var imageRect = NSRect(origin: .zero, size: image.size)
    guard let cgImage = image.cgImage(forProposedRect: &imageRect, context: nil, hints: nil) else {
        throw OCRFailure.imageLoadFailed
    }

    var recognizedLines: [OCRLine] = []
    var confidences: [Double] = []

    func isLikelyNoise(_ line: String) -> Bool {
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.count < 2 { return true }
        let letters = trimmed.unicodeScalars.filter { CharacterSet.letters.contains($0) }.count
        let digits = trimmed.unicodeScalars.filter { CharacterSet.decimalDigits.contains($0) }.count
        let symbols = trimmed.count - letters - digits
        if letters == 0 && digits < 2 { return true }
        // Heavy symbol ratio usually means OCR garbage.
        if symbols > letters && symbols > 3 { return true }
        return false
    }

    let request = VNRecognizeTextRequest { request, error in
        if let error = error {
            fputs("OCR failed: \(error.localizedDescription)\n", stderr)
            return
        }

        let observations = request.results as? [VNRecognizedTextObservation] ?? []
        for observation in observations {
            guard let candidate = observation.topCandidates(1).first else { continue }
            if candidate.confidence < 0.24 { continue }
            let line = candidate.string.trimmingCharacters(in: .whitespacesAndNewlines)
            if line.isEmpty { continue }
            if isLikelyNoise(line) { continue }
            let box = observation.boundingBox
            recognizedLines.append(
                OCRLine(
                    text: line,
                    confidence: Double(candidate.confidence),
                    x: Double(box.origin.x),
                    y: Double(box.origin.y)
                )
            )
            confidences.append(Double(candidate.confidence))
        }
    }

    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    let lang = (languageHint ?? "en").trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    if lang == "en" {
        request.recognitionLanguages = ["en-US"]
    } else {
        request.recognitionLanguages = [lang, "en-US"]
    }
    // Capture smaller UI text as well (menus, sidebars, fine-print labels).
    request.minimumTextHeight = 0.005

    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    try handler.perform([request])

    // Sort in reading order: top-to-bottom, then left-to-right.
    let sorted = recognizedLines.sorted { a, b in
        if abs(a.y - b.y) > 0.015 { return a.y > b.y }
        return a.x < b.x
    }
    let dedupedLines = Array(
        NSOrderedSet(array: sorted.map { $0.text.trimmingCharacters(in: .whitespacesAndNewlines) })
    ).compactMap { $0 as? String }
    let combined = dedupedLines.joined(separator: "\n")
    let avgConfidence = confidences.isEmpty ? 0.0 : (confidences.reduce(0.0, +) / Double(confidences.count))
    return OCRResult(text: combined, lines: dedupedLines, confidence: avgConfidence)
}

do {
    guard CommandLine.arguments.count >= 2 else {
        throw OCRFailure.invalidArguments
    }

    let languageHint = CommandLine.arguments.count >= 3 ? CommandLine.arguments[2] : nil
    let result = try runOCR(imagePath: CommandLine.arguments[1], languageHint: languageHint)
    let data = try JSONEncoder().encode(result)
    FileHandle.standardOutput.write(data)
} catch OCRFailure.invalidArguments {
    fputs("Usage: ocr_vision.swift <image-path>\n", stderr)
    exit(2)
} catch {
    fputs("OCR error: \(error.localizedDescription)\n", stderr)
    exit(1)
}
