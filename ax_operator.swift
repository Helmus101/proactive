import Foundation
import AppKit
import CoreGraphics
import ApplicationServices

struct RectData: Codable {
    let x: Double
    let y: Double
    let width: Double
    let height: Double
}

struct AXNodeData: Codable {
    let id: String
    let role: String
    let title: String
    let value: String
    let description: String
    let identifier: String
    let frame: RectData?
    let enabled: Bool?
    let focused: Bool?
    let selected: Bool?
    let actions: [String]
    let children: [AXNodeData]
}

struct SnapshotResult: Codable {
    let frontmost_app: String
    let window_title: String
    let window_id: UInt32?
    let bounds: RectData?
    let focused_element_id: String?
    let text_sample: String
    let status: String
    let ax_tree: AXNodeData?
}

struct ActionRequest: Codable {
    let kind: String
    let target_id: String?
    let target_point: RectData?
    let text: String?
    let key: String?
    let modifiers: [String]?
    let app: String?
    let title: String?
    let timeout_ms: Int?
}

struct ActionResult: Codable {
    let status: String
    let error: String?
    let result: String?
}

func encoder() -> JSONEncoder {
    let enc = JSONEncoder()
    enc.outputFormatting = [.withoutEscapingSlashes]
    return enc
}

func decodeInput<T: Decodable>(_ type: T.Type, from path: String) throws -> T {
    let data = try Data(contentsOf: URL(fileURLWithPath: path))
    return try JSONDecoder().decode(T.self, from: data)
}

func stringValue(_ value: AnyObject?) -> String {
    guard let value = value else { return "" }
    if CFGetTypeID(value) == AXUIElementGetTypeID() {
        return ""
    }
    if let str = value as? String {
        return str.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    if let attr = value as? NSAttributedString {
        return attr.string.trimmingCharacters(in: .whitespacesAndNewlines)
    }
    if let number = value as? NSNumber {
        return number.stringValue
    }
    return ""
}

func boolValue(_ value: AnyObject?) -> Bool? {
    guard let value = value else { return nil }
    if let n = value as? NSNumber {
        return n.boolValue
    }
    if let str = value as? String {
        if str.lowercased() == "true" { return true }
        if str.lowercased() == "false" { return false }
    }
    return nil
}

func rectFromAXValue(_ value: AnyObject?) -> RectData? {
    guard let value else { return nil }
    let typeID = CFGetTypeID(value)
    guard typeID == AXValueGetTypeID() else { return nil }
    let axValue = unsafeBitCast(value, to: AXValue.self)
    let axType = AXValueGetType(axValue)
    if axType == .cgRect {
      var rect = CGRect.zero
      if AXValueGetValue(axValue, .cgRect, &rect) {
        return RectData(x: rect.origin.x, y: rect.origin.y, width: rect.size.width, height: rect.size.height)
      }
    }
    if axType == .cgPoint {
      var point = CGPoint.zero
      if AXValueGetValue(axValue, .cgPoint, &point) {
        return RectData(x: point.x, y: point.y, width: 0, height: 0)
      }
    }
    if axType == .cgSize {
      var size = CGSize.zero
      if AXValueGetValue(axValue, .cgSize, &size) {
        return RectData(x: 0, y: 0, width: size.width, height: size.height)
      }
    }
    return nil
}

func cgWindowInfo(for pid: pid_t, title: String) -> (UInt32?, RectData?) {
    guard let infoList = CGWindowListCopyWindowInfo([.optionOnScreenOnly, .excludeDesktopElements], kCGNullWindowID) as? [[String: Any]] else {
        return (nil, nil)
    }
    let targetTitle = title.trimmingCharacters(in: .whitespacesAndNewlines)
    for info in infoList {
        let ownerPid = info[kCGWindowOwnerPID as String] as? pid_t
        let layer = info[kCGWindowLayer as String] as? Int ?? 0
        guard ownerPid == pid, layer == 0 else { continue }
        let name = (info[kCGWindowName as String] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if !targetTitle.isEmpty && !name.isEmpty && name != targetTitle { continue }
        if let boundsDict = info[kCGWindowBounds as String] as? NSDictionary {
            var rect = CGRect.zero
            if CGRectMakeWithDictionaryRepresentation(boundsDict, &rect), rect.width > 20, rect.height > 20 {
                return (
                    info[kCGWindowNumber as String] as? UInt32,
                    RectData(x: rect.origin.x, y: rect.origin.y, width: rect.width, height: rect.height)
                )
            }
        }
    }
    return (nil, nil)
}

func getAttribute(_ element: AXUIElement, _ attr: String) -> AnyObject? {
    var value: AnyObject?
    let result = AXUIElementCopyAttributeValue(element, attr as CFString, &value)
    return result == .success ? value : nil
}

func getElementAttribute(_ element: AXUIElement, _ attr: String) -> AXUIElement? {
    var value: AnyObject?
    let result = AXUIElementCopyAttributeValue(element, attr as CFString, &value)
    guard result == .success, let raw = value else { return nil }
    if CFGetTypeID(raw) == AXUIElementGetTypeID() {
        return unsafeBitCast(raw, to: AXUIElement.self)
    }
    return nil
}

func getActionNames(_ element: AXUIElement) -> [String] {
    var actions: CFArray?
    guard AXUIElementCopyActionNames(element, &actions) == .success,
          let arr = actions as? [String] else {
        return []
    }
    return arr
}

func elementFrame(_ element: AXUIElement) -> RectData? {
    let pos = rectFromAXValue(getAttribute(element, kAXPositionAttribute))
    let size = rectFromAXValue(getAttribute(element, kAXSizeAttribute))
    if let pos, let size {
        return RectData(x: pos.x, y: pos.y, width: size.width, height: size.height)
    }
    return nil
}

func buildNode(_ element: AXUIElement, path: String, depth: Int = 0, maxDepth: Int = 5, visited: inout Set<Int>) -> AXNodeData {
    let key = Int(bitPattern: Unmanaged.passUnretained(element).toOpaque())
    if visited.contains(key) || depth > maxDepth {
        return AXNodeData(id: path, role: "", title: "", value: "", description: "", identifier: "", frame: nil, enabled: nil, focused: nil, selected: nil, actions: [], children: [])
    }
    visited.insert(key)

    let role = stringValue(getAttribute(element, kAXRoleAttribute))
    let title = stringValue(getAttribute(element, kAXTitleAttribute))
    let value = stringValue(getAttribute(element, kAXValueAttribute))
    let description = stringValue(getAttribute(element, kAXDescriptionAttribute))
    let identifier = stringValue(getAttribute(element, kAXIdentifierAttribute))
    let enabled = boolValue(getAttribute(element, kAXEnabledAttribute))
    let focused = boolValue(getAttribute(element, kAXFocusedAttribute))
    let selected = boolValue(getAttribute(element, kAXSelectedAttribute))
    let actions = getActionNames(element)
    let frame = elementFrame(element)

    var childNodes: [AXNodeData] = []
    if let children = getAttribute(element, kAXChildrenAttribute) as? [AXUIElement], depth < maxDepth {
        for (index, child) in children.prefix(24).enumerated() {
            let childId = path == "root" ? "root.\(index)" : "\(path).\(index)"
            let node = buildNode(child, path: childId, depth: depth + 1, maxDepth: maxDepth, visited: &visited)
            childNodes.append(node)
        }
    }

    return AXNodeData(
        id: path,
        role: role,
        title: title,
        value: value,
        description: description,
        identifier: identifier,
        frame: frame,
        enabled: enabled,
        focused: focused,
        selected: selected,
        actions: actions,
        children: childNodes
    )
}

func collectText(_ node: AXNodeData, into lines: inout [String]) {
    for part in [node.title, node.value, node.description] {
        let cleaned = part.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression).trimmingCharacters(in: .whitespacesAndNewlines)
        if !cleaned.isEmpty && cleaned.count > 1 && !lines.contains(cleaned) {
            lines.append(cleaned)
        }
    }
    for child in node.children {
        if lines.count >= 80 { return }
        collectText(child, into: &lines)
    }
}

func findFocusedElementId(_ node: AXNodeData) -> String? {
    if node.focused == true {
        return node.id
    }
    for child in node.children {
        if let found = findFocusedElementId(child) {
            return found
        }
    }
    return nil
}

func resolveElementByPath(_ root: AXUIElement, path: String) -> AXUIElement? {
    let comps = path.split(separator: ".").map(String.init)
    guard comps.first == "root" else { return nil }
    var current = root
    if comps.count == 1 { return current }
    for comp in comps.dropFirst() {
        guard let index = Int(comp),
              let children = getAttribute(current, kAXChildrenAttribute) as? [AXUIElement],
              index >= 0, index < children.count else {
            return nil
        }
        current = children[index]
    }
    return current
}

func frontmostApplication() -> NSRunningApplication? {
    NSWorkspace.shared.frontmostApplication
}

func focusedWindow(of appElement: AXUIElement) -> AXUIElement? {
    getElementAttribute(appElement, kAXFocusedWindowAttribute)
}

func snapshotFrontmostWindow() -> SnapshotResult {
    guard let app = frontmostApplication() else {
        return SnapshotResult(frontmost_app: "", window_title: "", window_id: nil, bounds: nil, focused_element_id: nil, text_sample: "", status: "no_frontmost_app", ax_tree: nil)
    }
    let pid = app.processIdentifier
    let appElement = AXUIElementCreateApplication(pid)
    guard let window = focusedWindow(of: appElement) else {
        return SnapshotResult(frontmost_app: app.localizedName ?? "", window_title: "", window_id: nil, bounds: nil, focused_element_id: nil, text_sample: "", status: "no_focused_window", ax_tree: nil)
    }
    let title = stringValue(getAttribute(window, kAXTitleAttribute))
    var visited = Set<Int>()
    let tree = buildNode(window, path: "root", visited: &visited)
    var lines: [String] = []
    collectText(tree, into: &lines)
    let focusedId = findFocusedElementId(tree)
    let (windowId, cgBounds) = cgWindowInfo(for: pid, title: title)
    let bounds = tree.frame ?? cgBounds
    return SnapshotResult(
        frontmost_app: app.localizedName ?? "",
        window_title: title,
        window_id: windowId,
        bounds: bounds,
        focused_element_id: focusedId,
        text_sample: Array(lines.prefix(50)).joined(separator: "\n"),
        status: "complete",
        ax_tree: tree
    )
}

func performPress(on element: AXUIElement) -> Bool {
    for action in [kAXPressAction as String, "AXConfirm"] {
        if AXUIElementPerformAction(element, action as CFString) == .success {
            return true
        }
    }
    return false
}

func focusElement(_ element: AXUIElement) -> Bool {
    let cfTrue: CFTypeRef = kCFBooleanTrue
    if AXUIElementSetAttributeValue(element, kAXFocusedAttribute as CFString, cfTrue) == .success {
        return true
    }
    return performPress(on: element)
}

func setElementValue(_ element: AXUIElement, text: String) -> Bool {
    if AXUIElementSetAttributeValue(element, kAXValueAttribute as CFString, text as CFTypeRef) == .success {
        return true
    }
    return false
}

func postMouseClick(at point: CGPoint) {
    guard let source = CGEventSource(stateID: .hidSystemState) else { return }
    let down = CGEvent(mouseEventSource: source, mouseType: .leftMouseDown, mouseCursorPosition: point, mouseButton: .left)
    let up = CGEvent(mouseEventSource: source, mouseType: .leftMouseUp, mouseCursorPosition: point, mouseButton: .left)
    down?.post(tap: .cghidEventTap)
    up?.post(tap: .cghidEventTap)
}

func postText(_ text: String) {
    for scalar in text.unicodeScalars {
        guard let source = CGEventSource(stateID: .hidSystemState),
              let keyDown = CGEvent(keyboardEventSource: source, virtualKey: 0, keyDown: true),
              let keyUp = CGEvent(keyboardEventSource: source, virtualKey: 0, keyDown: false) else {
            continue
        }
        var chars = [UniChar(scalar.value)]
        keyDown.keyboardSetUnicodeString(stringLength: 1, unicodeString: &chars)
        keyUp.keyboardSetUnicodeString(stringLength: 1, unicodeString: &chars)
        keyDown.post(tap: .cghidEventTap)
        keyUp.post(tap: .cghidEventTap)
    }
}

func keyCode(for key: String) -> CGKeyCode? {
    switch key.lowercased() {
    case "enter", "return": return 36
    case "tab": return 48
    case "space": return 49
    case "escape", "esc": return 53
    case "left": return 123
    case "right": return 124
    case "down", "pagedown", "page_down": return 125
    case "up", "pageup", "page_up": return 126
    default: return nil
    }
}

func modifierFlags(_ modifiers: [String]) -> CGEventFlags {
    var flags: CGEventFlags = []
    for modifier in modifiers.map({ $0.lowercased() }) {
        switch modifier {
        case "cmd", "command": flags.insert(.maskCommand)
        case "ctrl", "control": flags.insert(.maskControl)
        case "shift": flags.insert(.maskShift)
        case "option", "alt": flags.insert(.maskAlternate)
        default: break
        }
    }
    return flags
}

func postKey(_ key: String, modifiers: [String]) {
    guard let source = CGEventSource(stateID: .hidSystemState) else { return }
    let flags = modifierFlags(modifiers)
    if let code = keyCode(for: key),
       let down = CGEvent(keyboardEventSource: source, virtualKey: code, keyDown: true),
       let up = CGEvent(keyboardEventSource: source, virtualKey: code, keyDown: false) {
        down.flags = flags
        up.flags = flags
        down.post(tap: .cghidEventTap)
        up.post(tap: .cghidEventTap)
        return
    }
    postText(key)
}

func activateApp(named appName: String?) -> Bool {
    guard let appName, !appName.isEmpty else { return false }
    let apps = NSRunningApplication.runningApplications(withBundleIdentifier: appName)
    if let match = apps.first {
        return match.activate(options: [])
    }
    if let match = NSWorkspace.shared.runningApplications.first(where: { ($0.localizedName ?? "").caseInsensitiveCompare(appName) == .orderedSame }) {
        return match.activate(options: [])
    }
    return false
}

func performAction(_ request: ActionRequest) -> ActionResult {
    let snapshot = snapshotFrontmostWindow()
    guard let tree = snapshot.ax_tree else {
        return ActionResult(status: "error", error: "no_ax_tree_available", result: nil)
    }
    let app = frontmostApplication()
    let appElement = app.map { AXUIElementCreateApplication($0.processIdentifier) }
    let window = appElement.flatMap { focusedWindow(of: $0) }

    switch request.kind.uppercased() {
    case "ACTIVATE_APP":
        return activateApp(named: request.app) ? ActionResult(status: "success", error: nil, result: "app activated") : ActionResult(status: "error", error: "app_not_found", result: nil)
    case "FOCUS_WINDOW":
        if let window, performPress(on: window) {
            return ActionResult(status: "success", error: nil, result: "window focused")
        }
        return ActionResult(status: "error", error: "window_not_found", result: nil)
    case "PRESS_AX", "CLICK_AX":
        guard let window, let targetId = request.target_id, let target = resolveElementByPath(window, path: targetId) else {
            return ActionResult(status: "error", error: "target_not_found", result: nil)
        }
        if performPress(on: target) {
            return ActionResult(status: "success", error: nil, result: "pressed")
        }
        return ActionResult(status: "error", error: "ax_press_failed", result: nil)
    case "FOCUS_AX":
        guard let window, let targetId = request.target_id, let target = resolveElementByPath(window, path: targetId) else {
            return ActionResult(status: "error", error: "target_not_found", result: nil)
        }
        return focusElement(target) ? ActionResult(status: "success", error: nil, result: "focused") : ActionResult(status: "error", error: "ax_focus_failed", result: nil)
    case "SET_AX_VALUE":
        guard let window, let targetId = request.target_id, let target = resolveElementByPath(window, path: targetId) else {
            return ActionResult(status: "error", error: "target_not_found", result: nil)
        }
        if setElementValue(target, text: request.text ?? "") {
            return ActionResult(status: "success", error: nil, result: "value_set")
        }
        if focusElement(target) {
            postText(request.text ?? "")
            return ActionResult(status: "success", error: nil, result: "typed_fallback")
        }
        return ActionResult(status: "error", error: "set_value_failed", result: nil)
    case "TYPE_TEXT":
        postText(request.text ?? "")
        return ActionResult(status: "success", error: nil, result: "typed")
    case "KEY_PRESS":
        postKey(request.key ?? "", modifiers: request.modifiers ?? [])
        return ActionResult(status: "success", error: nil, result: "key_pressed")
    case "SCROLL_AX":
        let key = (request.key ?? "").isEmpty ? "pagedown" : request.key!
        postKey(key, modifiers: [])
        return ActionResult(status: "success", error: nil, result: "scrolled")
    case "CLICK_POINT":
        guard let point = request.target_point else {
            return ActionResult(status: "error", error: "missing_target_point", result: nil)
        }
        postMouseClick(at: CGPoint(x: point.x, y: point.y))
        return ActionResult(status: "success", error: nil, result: "point_clicked")
    default:
        _ = tree
        return ActionResult(status: "error", error: "unsupported_action", result: nil)
    }
}

let args = CommandLine.arguments
guard args.count >= 2 else {
    fputs("usage: ax_operator.swift snapshot|action [json-file]\n", stderr)
    exit(1)
}

let mode = args[1]
do {
    if mode == "snapshot" {
        let data = try encoder().encode(snapshotFrontmostWindow())
        FileHandle.standardOutput.write(data)
    } else if mode == "action" {
        guard args.count >= 3 else {
            throw NSError(domain: "AXOperator", code: 1, userInfo: [NSLocalizedDescriptionKey: "missing action input"])
        }
        let request = try decodeInput(ActionRequest.self, from: args[2])
        let data = try encoder().encode(performAction(request))
        FileHandle.standardOutput.write(data)
    } else {
        throw NSError(domain: "AXOperator", code: 1, userInfo: [NSLocalizedDescriptionKey: "unknown mode"])
    }
} catch {
    fputs("AX operator error: \(error.localizedDescription)\n", stderr)
    exit(1)
}
