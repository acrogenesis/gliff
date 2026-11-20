#!/usr/bin/env python3
"""Exercise production RemoteView input handling without posting OS input."""
import subprocess
import sys
import tempfile
from pathlib import Path


def main():
    if sys.platform != "darwin":
        sys.exit("Run this check on macOS.")
    source = Path(__file__).resolve().parents[1] / "crates/gliff-macos/native/Gliff.swift"
    text = source.read_text().split("let mode = ProcessInfo", 1)[0]
    old = '''func event(_ value: [String: Any]) {
    if let data = try? JSONSerialization.data(withJSONObject: value) { packet(2, data) }
}'''
    assert old in text, "cannot locate bridge event sink"
    text = text.replace(old, '''var observedEvents: [[String: Any]] = []
func event(_ value: [String: Any]) { observedEvents.append(value) }''')
    text = text.replace('e?.post(tap: .cghidEventTap)', 'if let e = e { observedNativeEvents.append(e) }')
    text = 'var observedNativeEvents: [CGEvent] = []\n' + text
    text += '''
func keyEvent(_ code: UInt16, _ flags: NSEvent.ModifierFlags = [], _ type: NSEvent.EventType = .keyDown) -> NSEvent {
    NSEvent.keyEvent(with: type, location: .zero, modifierFlags: flags, timestamp: 0, windowNumber: 0, context: nil, characters: "", charactersIgnoringModifiers: "", isARepeat: false, keyCode: code)!
}
let view = RemoteView(frame: NSRect(x: 0, y: 0, width: 100, height: 100))
view.keyDown(with: keyEvent(0))
view.flagsChanged(with: keyEvent(56, .shift, .flagsChanged))
precondition(observedEvents.isEmpty, "keys escaped the picture boundary")
view.hovering = true
view.flagsChanged(with: keyEvent(57, .capsLock, .flagsChanged))
view.flagsChanged(with: keyEvent(57, [], .flagsChanged))
precondition(observedEvents.count == 4, "Caps Lock did not pulse on both transitions")
precondition(observedEvents.map { $0["pressed"] as! Bool } == [true, false, true, false])
observedEvents.removeAll()
view.keyDown(with: keyEvent(0))
view.flagsChanged(with: keyEvent(56, .shift, .flagsChanged))
view.buttons.insert(0)
view.keyDown(with: keyEvent(53, .shift))
precondition(view.released && view.pressed.isEmpty && view.buttons.isEmpty, "hotkey left held input")
precondition(observedEvents.filter { $0["type"] as? String == "key" && $0["pressed"] as? Bool == false }.count == 2)
precondition(observedEvents.contains { $0["type"] as? String == "button" && $0["pressed"] as? Bool == false })
precondition(!observedEvents.contains { $0["keycode"] as? UInt16 == 53 }, "release hotkey reached host")
observedEvents.removeAll()
view.keyDown(with: keyEvent(1))
precondition(observedEvents.isEmpty, "released viewer forwarded a key")
view.released = false
let imageBytes = Data(repeating: 0, count: 8)
view.image = CGImage(width: 2, height: 1, bitsPerComponent: 8, bitsPerPixel: 32, bytesPerRow: 8, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGBitmapInfo(rawValue: CGImageAlphaInfo.noneSkipFirst.rawValue), provider: CGDataProvider(data: imageBytes as CFData)!, decode: nil, shouldInterpolate: false, intent: .defaultIntent)
view.hovering = true
view.keyDown(with: keyEvent(0))
observedEvents.removeAll()
let outside = NSEvent.mouseEvent(with: .mouseMoved, location: NSPoint(x: 1, y: 1), modifierFlags: [], timestamp: 0, windowNumber: 0, context: nil, eventNumber: 0, clickCount: 0, pressure: 0)!
view.mouseMoved(with: outside)
precondition(!view.hovering && view.pressed.isEmpty, "leaving the picture did not release keys")
precondition(observedEvents.count == 1 && observedEvents[0]["pressed"] as? Bool == false)
observedEvents.removeAll()
view.keyDown(with: keyEvent(1))
precondition(observedEvents.isEmpty, "letterbox key reached the host")
func clickEvent(_ type: NSEvent.EventType, _ point: NSPoint) -> NSEvent {
    NSEvent.mouseEvent(with: type, location: point, modifierFlags: [], timestamp: 0, windowNumber: 0, context: nil, eventNumber: 0, clickCount: 1, pressure: 1)!
}
let barPoint = NSPoint(x: 10, y: 10)
let picturePoint = NSPoint(x: 50, y: 50)
view.mouseDown(with: clickEvent(.leftMouseDown, barPoint))
view.mouseUp(with: clickEvent(.leftMouseUp, barPoint))
precondition(observedEvents.isEmpty && view.buttons.isEmpty, "letterbox click reached the host")
view.mouseDown(with: clickEvent(.leftMouseDown, picturePoint))
view.mouseUp(with: clickEvent(.leftMouseUp, barPoint))
let clicks = observedEvents.filter { $0["type"] as? String == "button" }
precondition(clicks.map { $0["pressed"] as! Bool } == [true, false], "a valid press was not released outside the picture")
precondition(view.buttons.isEmpty)
let app = NSApplication.shared
let delegate = App()
let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1470, height: 956), styleMask: [.titled, .resizable], backing: .buffered, defer: false)
window.contentView = delegate.view; delegate.window = window
let pixels = Data(repeating: 0, count: 1470 * 956 * 4)
delegate.view.image = CGImage(width: 1470, height: 956, bitsPerComponent: 8, bitsPerPixel: 32, bytesPerRow: 1470 * 4, space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGBitmapInfo(rawValue: CGImageAlphaInfo.noneSkipFirst.rawValue), provider: CGDataProvider(data: pixels as CFData)!, decode: nil, shouldInterpolate: false, intent: .defaultIntent)
precondition(delegate.view.pictureRect == delegate.view.bounds, "A matching private desktop did not fill the viewport")
observedEvents.removeAll()
let note = Notification(name: NSWindow.didEnterFullScreenNotification, object: window)
delegate.windowDidEnterFullScreen(note)
delegate.windowDidExitFullScreen(note)
delegate.windowDidChangeBackingProperties(note)
precondition(observedEvents.count == 3 && observedEvents.allSatisfy { $0["type"] as? String == "resize" }, "Fullscreen or backing changes did not update the remote viewport")
let input = Input()
input.capsLock = false
input.handle(["type": "key", "keycode": UInt16(57), "pressed": true])
input.handle(["type": "key", "keycode": UInt16(57), "pressed": true])
input.handle(["type": "key", "keycode": UInt16(57), "pressed": false])
input.handle(["type": "key", "keycode": UInt16(0), "pressed": true])
precondition(observedNativeEvents.allSatisfy { $0.flags.contains(.maskAlphaShift) }, "host lost Caps Lock state")
observedNativeEvents.removeAll()
input.handle(["type": "key", "keycode": UInt16(57), "pressed": true])
input.handle(["type": "key", "keycode": UInt16(57), "pressed": false])
input.handle(["type": "key", "keycode": UInt16(0), "pressed": false])
precondition(observedNativeEvents.allSatisfy { !$0.flags.contains(.maskAlphaShift) }, "host did not turn Caps Lock off")
print("INPUT PASS: picture boundary, Caps Lock client/host state, Shift+Esc, held-input release, private viewport fill and fullscreen/backing resize")
'''
    with tempfile.TemporaryDirectory(prefix="gliff-input-check-") as folder:
        swift = Path(folder, "main.swift")
        binary = Path(folder, "input-check")
        swift.write_text(text)
        subprocess.run(["xcrun", "swiftc", "-O", "-swift-version", "5", str(swift), "-o", str(binary)], check=True, timeout=60)
        subprocess.run([str(binary)], check=True, timeout=10)


if __name__ == "__main__":
    main()
