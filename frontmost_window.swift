import Foundation
import AppKit
import CoreGraphics
import ApplicationServices

struct WindowContext: Encodable {
    let appName: String
    let windowTitle: String
    let windowId: UInt32?
    let x: Double?
    let y: Double?
    let width: Double?
    let height: Double?
    let extractedText: String
    let status: String
}

func textFromAXValue(_ value: AnyObject?) -> String {
    guard let value = value else { return "" }
    if CFGetTypeID(value) == AXUIElementGetTypeID() {
        return ""
    }
    if let str = value as? String {
        return str.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    if let attributed = value as? NSAttributedString {
        return attributed.string.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    return ""
}

func collectAXText(_ element: AXUIElement, visited: inout Set<Int>, depth: Int = 0, maxDepth: Int = 4, limit: Int = 80) -> [String] {
    if depth > maxDepth || visited.count > 250 { return [] }
    let key = Int(bitPattern: Unmanaged.passUnretained(element).toOpaque())
    if visited.contains(key) { return [] }
    visited.insert(key)

    var out: [String] = []

    var roleValue: AnyObject?
    AXUIElementCopyAttributeValue(element, kAXRoleAttribute as CFString, &roleValue)
    let role = textFromAXValue(roleValue).lowercased()

    let captureByRole = role.contains("statictext") || role.contains("textfield") || role.contains("textarea") || role.contains("text")
    if captureByRole {
        var valueAttr: AnyObject?
        if AXUIElementCopyAttributeValue(element, kAXValueAttribute as CFString, &valueAttr) == .success {
            let text = textFromAXValue(valueAttr)
            if !text.isEmpty { out.append(text) }
        }
    }

    var titleAttr: AnyObject?
    if AXUIElementCopyAttributeValue(element, kAXTitleAttribute as CFString, &titleAttr) == .success {
        let title = textFromAXValue(titleAttr)
        if !title.isEmpty && title.count > 2 { out.append(title) }
    }

    if out.count >= limit { return Array(out.prefix(limit)) }

    var childrenValue: AnyObject?
    if AXUIElementCopyAttributeValue(element, kAXChildrenAttribute as CFString, &childrenValue) == .success,
       let children = childrenValue as? [AXUIElement] {
        for child in children {
            let nested = collectAXText(child, visited: &visited, depth: depth + 1, maxDepth: maxDepth, limit: max(0, limit - out.count))
            out.append(contentsOf: nested)
            if out.count >= limit { break }
        }
    }

    return Array(out.prefix(limit))
}

func getFrontmostWindowExtractedText(pid: pid_t) -> String {
    let appElement = AXUIElementCreateApplication(pid)
    var focusedWindow: AnyObject?
    guard AXUIElementCopyAttributeValue(appElement, kAXFocusedWindowAttribute as CFString, &focusedWindow) == .success,
          let focusedWindow else {
        return ""
    }
    let window = focusedWindow as! AXUIElement

    var visited = Set<Int>()
    let lines = collectAXText(window, visited: &visited, depth: 0, maxDepth: 4, limit: 90)
        .map { $0.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression) }
        .filter { !$0.isEmpty }

    let deduped = Array(NSOrderedSet(array: lines)) as? [String] ?? lines
    return deduped.prefix(50).joined(separator: "\n")
}

func frontmostWindowContext() -> WindowContext {
    guard let app = NSWorkspace.shared.frontmostApplication else {
        return WindowContext(appName: "", windowTitle: "", windowId: nil, x: nil, y: nil, width: nil, height: nil, extractedText: "", status: "no_frontmost_app")
    }

    let pid = app.processIdentifier
    let appName = app.localizedName ?? ""
    let extractedText = getFrontmostWindowExtractedText(pid: pid)
    guard let infoList = CGWindowListCopyWindowInfo([.optionOnScreenOnly, .excludeDesktopElements], kCGNullWindowID) as? [[String: Any]] else {
        return WindowContext(appName: appName, windowTitle: "", windowId: nil, x: nil, y: nil, width: nil, height: nil, extractedText: extractedText, status: "window_query_failed")
    }

    for info in infoList {
        let ownerPid = info[kCGWindowOwnerPID as String] as? pid_t
        let layer = info[kCGWindowLayer as String] as? Int ?? 0
        guard ownerPid == pid, layer == 0 else { continue }

        let boundsDict = info[kCGWindowBounds as String] as? NSDictionary
        var bounds = CGRect.zero
        if let boundsDict, CGRectMakeWithDictionaryRepresentation(boundsDict, &bounds), bounds.width > 20, bounds.height > 20 {
            let windowId = info[kCGWindowNumber as String] as? UInt32
            let title = (info[kCGWindowName as String] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
            return WindowContext(
                appName: appName,
                windowTitle: title,
                windowId: windowId,
                x: bounds.origin.x,
                y: bounds.origin.y,
                width: bounds.width,
                height: bounds.height,
                extractedText: extractedText,
                status: "complete"
            )
        }
    }

    return WindowContext(appName: appName, windowTitle: "", windowId: nil, x: nil, y: nil, width: nil, height: nil, extractedText: extractedText, status: "no_window")
}

let encoder = JSONEncoder()
encoder.outputFormatting = [.withoutEscapingSlashes]

do {
    let data = try encoder.encode(frontmostWindowContext())
    FileHandle.standardOutput.write(data)
} catch {
    fputs("Window context error: \(error.localizedDescription)\n", stderr)
    exit(1)
}
