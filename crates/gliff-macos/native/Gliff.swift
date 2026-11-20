import AppKit
import ScreenCaptureKit
import CoreMedia
import CoreVideo
import ApplicationServices
import VideoToolbox
import Carbon

// stdout belongs exclusively to the bridge. Diagnostics go to stderr.
let outputLock = NSLock()
func packet(_ kind: UInt8, _ data: Data) {
    outputLock.lock()
    defer { outputLock.unlock() }
    var header = Data([kind])
    var count = UInt32(data.count).littleEndian
    withUnsafeBytes(of: &count) { header.append(contentsOf: $0) }
    do {
        try FileHandle.standardOutput.write(contentsOf: header)
        try FileHandle.standardOutput.write(contentsOf: data)
    } catch { exit(0) }
}
func event(_ value: [String: Any]) {
    if let data = try? JSONSerialization.data(withJSONObject: value) { packet(2, data) }
}
func diagnostic(_ text: String) {
    FileHandle.standardError.write(Data((text + "\n").utf8))
}
func fail(_ text: String) -> Never {
    event(["type": "error", "message": text]); diagnostic(text); exit(1)
}
func exact(_ count: Int) -> Data? {
    // Pipe reads can return only a few KiB even for a Retina-size frame.
    // Requesting the entire remainder churns large allocations that malloc
    // caches after each short read; reserve once and keep read buffers small.
    var data = Data(capacity: count)
    while data.count < count {
        // Drain Foundation's temporary NSData after each short read instead of
        // keeping a frame's worth of chunk buffers until the packet completes.
        let received = autoreleasepool { () -> Bool in
            guard let part = try? FileHandle.standardInput.read(upToCount: min(count - data.count, 64 * 1024)), !part.isEmpty else { return false }
            data.append(part); return true
        }
        if !received { return nil }
    }
    return data
}
func number(_ data: Data, _ at: Int) -> UInt32 {
    data[at..<at+4].enumerated().reduce(UInt32(0)) { $0 | UInt32($1.element) << (8 * $1.offset) }
}
func append(_ number: UInt32, _ to: inout Data) {
    var n = number.littleEndian
    withUnsafeBytes(of: &n) { to.append(contentsOf: $0) }
}
func readCommands(onClose: @escaping () -> Void = {}, _ callback: @escaping (UInt8, Data) -> Void) {
    DispatchQueue.global(qos: .userInteractive).async {
        // This dispatch work item lives for the entire connection. Foundation
        // pipe reads autorelease NSData, so drain each packet, not just on exit.
        while autoreleasepool(invoking: { () -> Bool in
            guard let header = exact(5) else { return false }
            let count = Int(number(header, 1))
            guard count <= 64 * 1024 * 1024, let data = exact(count) else { return false }
            callback(header[0], data)
            return true
        }) {}
        DispatchQueue.main.async { onClose(); NSApplication.shared.terminate(nil); exit(0) }
    }
}
func json(_ data: Data) -> [String: Any] {
    (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] ?? [:]
}

// Read a Unicode layout without selecting it or changing the user's input source.
// Keep the actual Mac symbols, including Option levels and dead keys, rather
// than guessing an XKB layout from the input source's display name.
func keyboardLayout(_ identifier: String? = nil) -> [String: Any]? {
    let source: TISInputSource
    if let identifier = identifier {
        guard let list = TISCreateInputSourceList([kTISPropertyInputSourceID as String: identifier] as CFDictionary, true)?.takeRetainedValue() else { return nil }
        let matches = list as! [TISInputSource]
        guard let first = matches.first else { return nil }
        source = first
    } else {
        guard let current = TISCopyCurrentKeyboardLayoutInputSource()?.takeRetainedValue() else { return nil }
        source = current
    }
    guard let raw = TISGetInputSourceProperty(source, kTISPropertyUnicodeKeyLayoutData) else { return nil }
    let data = Unmanaged<CFData>.fromOpaque(raw).takeUnretainedValue()
    guard let bytes = CFDataGetBytePtr(data) else { return nil }
    let layout = UnsafeRawPointer(bytes).assumingMemoryBound(to: UCKeyboardLayout.self)
    var keys: [[String: Any]] = []
    for code in UInt16(0)..<128 {
        var levels: [[String: Any]] = []
        for level in 0..<8 {
            let flags = (level & 1 == 0 ? 0 : shiftKey) | (level & 2 == 0 ? 0 : optionKey) | (level & 4 == 0 ? 0 : alphaLock)
            var state: UInt32 = 0
            var length = 0
            var chars = [UniChar](repeating: 0, count: 16)
            var status = UCKeyTranslate(layout, code, UInt16(kUCKeyActionDown), UInt32(flags >> 8), UInt32(LMGetKbdType()), 0, &state, chars.count, &length, &chars)
            let dead = state != 0
            if dead {
                state = 0
                status = UCKeyTranslate(layout, code, UInt16(kUCKeyActionDisplay), UInt32(flags >> 8), UInt32(LMGetKbdType()), OptionBits(kUCKeyTranslateNoDeadKeysMask), &state, chars.count, &length, &chars)
            }
            levels.append(["text": status == noErr && length <= chars.count ? String(utf16CodeUnits: chars, count: length) : "", "dead": dead])
        }
        keys.append(["keycode": Int(code), "levels": levels])
    }
    let name = TISGetInputSourceProperty(source, kTISPropertyInputSourceID).map { Unmanaged<CFString>.fromOpaque($0).takeUnretainedValue() as String } ?? "unknown"
    return ["type": "keyboard_layout", "source": name, "keys": keys]
}

// NSPasteboard requests data only when an application pastes. The pipe reader
// resolves the wait directly, so a paste callback on the main thread can wait.
final class PasteProvider: NSObject, NSPasteboardItemDataProvider {
    let serial: UInt32
    let condition = NSCondition()
    var text: String?
    var finished = false
    var requested = false
    init(_ serial: UInt32) { self.serial = serial }
    func pasteboard(_ pasteboard: NSPasteboard?, item: NSPasteboardItem, provideDataForType type: NSPasteboard.PasteboardType) {
        condition.lock()
        if !requested { requested = true; event(["type": "paste", "serial": serial]) }
        let deadline = Date(timeIntervalSinceNow: 30)
        while !finished { if !condition.wait(until: deadline) { break } }
        let value = text
        condition.unlock()
        if let value = value { item.setString(value, forType: type) }
    }
    func receive(_ cmd: [String: Any]) {
        condition.lock(); text = cmd["text"] as? String; finished = true
        condition.broadcast(); condition.unlock()
    }
}
final class Clipboard {
    var provider: PasteProvider?
    var observed = Int.min
    var timer: Timer?
    let lock = NSLock()
    func start() {
        timer = Timer.scheduledTimer(withTimeInterval: 0.5, repeats: true) { [weak self] _ in self?.poll() }
    }
    func poll() {
        let pb = NSPasteboard.general
        guard pb.changeCount != observed else { return }
        observed = pb.changeCount
        lock.lock(); provider = nil; lock.unlock()
        let text = pb.string(forType: .string)
        event(["type": "clipboard", "text": (text.map { $0.utf8.count <= 1024*1024 ? $0 : "" } as Any?) ?? NSNull()])
    }
    // Called on the pipe reader, including while the main thread is pasting.
    func receive(_ cmd: [String: Any]) -> Bool {
        guard cmd["type"] as? String == "clipboard_data" else { return false }
        lock.lock(); let p = provider; lock.unlock()
        if cmd["serial"] as? UInt32 == p?.serial { p?.receive(cmd) }
        return true
    }
    func command(_ cmd: [String: Any]) {
        guard cmd["type"] as? String == "clipboard_offer" else { return }
        let pb = NSPasteboard.general
        pb.clearContents()
        lock.lock()
        if cmd["text"] as? Bool == true, let serial = cmd["serial"] as? UInt32 {
            let p = PasteProvider(serial); let item = NSPasteboardItem()
            item.setDataProvider(p, forTypes: [.string]); provider = p
            pb.writeObjects([item])
        } else { provider = nil }
        lock.unlock()
        observed = pb.changeCount
    }
}
let clipboard = Clipboard()

final class Input {
    var display: CGDirectDisplayID = CGMainDisplayID()
    var keys: Set<CGKeyCode> = []
    var buttons: Set<Int> = []
    var position: CGPoint = .zero
    var capsLock = CGEventSource.flagsState(.combinedSessionState).contains(.maskAlphaShift)
    let source = CGEventSource(stateID: .privateState)
    func flags() -> CGEventFlags {
        var f: CGEventFlags = []
        if capsLock { f.insert(.maskAlphaShift) }
        if keys.contains(55) || keys.contains(54) { f.insert(.maskCommand) }
        if keys.contains(56) || keys.contains(60) { f.insert(.maskShift) }
        if keys.contains(58) || keys.contains(61) { f.insert(.maskAlternate) }
        if keys.contains(59) || keys.contains(62) { f.insert(.maskControl) }
        return f
    }
    func handle(_ cmd: [String: Any]) {
        switch cmd["type"] as? String {
        case "key":
            guard let code = cmd["keycode"] as? UInt16, let down = cmd["pressed"] as? Bool else { return }
            if code == 57 && down && !keys.contains(code) { capsLock.toggle() }
            if down { keys.insert(code) } else { keys.remove(code) }
            let e = CGEvent(keyboardEventSource: source, virtualKey: code, keyDown: down)
            e?.flags = flags(); e?.post(tap: .cghidEventTap)
        case "motion":
            let bounds = CGDisplayBounds(display)
            let x = min(max(cmd["x"] as? Double ?? 0, 0), max(0, bounds.width - 1))
            let y = min(max(cmd["y"] as? Double ?? 0, 0), max(0, bounds.height - 1))
            position = CGPoint(x: bounds.minX + x, y: bounds.minY + y)
            let type: CGEventType = buttons.contains(0) ? .leftMouseDragged : buttons.contains(1) ? .rightMouseDragged : buttons.isEmpty ? .mouseMoved : .otherMouseDragged
            let e = CGEvent(mouseEventSource: source, mouseType: type, mouseCursorPosition: position, mouseButton: buttons.contains(1) ? .right : .left)
            e?.flags = flags(); e?.post(tap: .cghidEventTap)
        case "button":
            guard let button = cmd["button"] as? Int, let down = cmd["pressed"] as? Bool, let b = CGMouseButton(rawValue: UInt32(button)) else { return }
            if down { buttons.insert(button) } else { buttons.remove(button) }
            let type: CGEventType = button == 0 ? (down ? .leftMouseDown : .leftMouseUp) : button == 1 ? (down ? .rightMouseDown : .rightMouseUp) : (down ? .otherMouseDown : .otherMouseUp)
            let e = CGEvent(mouseEventSource: source, mouseType: type, mouseCursorPosition: position, mouseButton: b)
            e?.flags = flags(); e?.post(tap: .cghidEventTap)
        case "axis":
            let value = Int32(max(-32768, min(32767, cmd["value"] as? Double ?? 0)))
            let horizontal = cmd["horizontal"] as? Bool ?? false
            let e = CGEvent(scrollWheelEvent2Source: source, units: .pixel, wheelCount: 2, wheel1: horizontal ? 0 : -value, wheel2: horizontal ? -value : 0, wheel3: 0)
            e?.flags = flags(); e?.post(tap: .cghidEventTap)
        case "release":
            for key in Array(keys) { handle(["type": "key", "keycode": key, "pressed": false]) }
            for button in Array(buttons) { handle(["type": "button", "button": button, "pressed": false]) }
        default: break
        }
    }
}

// Each request completes before accepting another frame. This bounds retained
// VideoToolbox surfaces even if the SSH client stops acknowledging video.
final class HardwareEncoder {
    var session: VTCompressionSession?
    let width: Int, height: Int, fps: Int
    var sequence: Int64 = 0
    var produced = false
    init(_ width: Int, _ height: Int, _ bitrate: Int, _ fps: Int) throws {
        self.width = width; self.height = height; self.fps = fps
        guard width >= 2, width <= 1920, height >= 2, height <= 1200, width % 2 == 0, height % 2 == 0, fps > 0 else { throw NSError(domain: "Gliff.Encoder", code: 1) }
        let specification = [kVTVideoEncoderSpecification_RequireHardwareAcceleratedVideoEncoder: true] as CFDictionary
        let attributes = [kCVPixelBufferPixelFormatTypeKey: kCVPixelFormatType_32BGRA,
                          kCVPixelBufferWidthKey: width, kCVPixelBufferHeightKey: height,
                          kCVPixelBufferIOSurfacePropertiesKey: [:]] as CFDictionary
        let callback: VTCompressionOutputCallback = { refcon, _, status, flags, sample in
            guard let refcon = refcon else { return }
            let encoder = Unmanaged<HardwareEncoder>.fromOpaque(refcon).takeUnretainedValue()
            autoreleasepool { encoder.output(status, flags, sample) }
        }
        do {
            try check(VTCompressionSessionCreate(allocator: kCFAllocatorDefault, width: Int32(width), height: Int32(height), codecType: kCMVideoCodecType_H264,
                encoderSpecification: specification, imageBufferAttributes: attributes, compressedDataAllocator: nil,
                outputCallback: callback, refcon: Unmanaged.passUnretained(self).toOpaque(), compressionSessionOut: &session), "create hardware encoder")
            guard let session = session else { throw NSError(domain: "Gliff.Encoder", code: 2) }
            for (key, value) in [(kVTCompressionPropertyKey_RealTime, true as CFBoolean),
                                 (kVTCompressionPropertyKey_AllowFrameReordering, false as CFBoolean)] {
                try check(VTSessionSetProperty(session, key: key, value: value), "set \(key)")
            }
            try check(VTSessionSetProperty(session, key: kVTCompressionPropertyKey_ProfileLevel, value: kVTProfileLevel_H264_Baseline_AutoLevel), "set H.264 profile")
            try check(VTSessionSetProperty(session, key: kVTCompressionPropertyKey_AverageBitRate, value: bitrate as CFNumber), "set bitrate")
            try check(VTSessionSetProperty(session, key: kVTCompressionPropertyKey_ExpectedFrameRate, value: fps as CFNumber), "set frame rate")
            try check(VTSessionSetProperty(session, key: kVTCompressionPropertyKey_MaxKeyFrameInterval, value: (fps * 2) as CFNumber), "set keyframe interval")
            try check(VTCompressionSessionPrepareToEncodeFrames(session), "prepare hardware encoder")
            var hardware: CFTypeRef?
            let hardwareStatus = withUnsafeMutablePointer(to: &hardware) { pointer in
                VTSessionCopyProperty(session, key: kVTCompressionPropertyKey_UsingHardwareAcceleratedVideoEncoder, allocator: nil, valueOut: UnsafeMutableRawPointer(pointer))
            }
            try check(hardwareStatus, "check hardware encoder")
            guard (hardware as? Bool) == true else { throw NSError(domain: "Gliff.Encoder", code: 3, userInfo: [NSLocalizedDescriptionKey: "VideoToolbox selected a software encoder"]) }
        } catch { close(); throw error }
    }
    func check(_ status: OSStatus, _ operation: String) throws {
        if status != noErr { throw NSError(domain: "Gliff.Encoder", code: Int(status), userInfo: [NSLocalizedDescriptionKey: "\(operation) failed (\(status))"]) }
    }
    func close() {
        if let session = session { VTCompressionSessionInvalidate(session); self.session = nil }
    }
    deinit { close() }
    func encode(_ data: Data) {
        guard data.count == 16 + width * height * 4, number(data, 0) == width, number(data, 4) == height, number(data, 12) <= 1,
              let session = session, let pool = VTCompressionSessionGetPixelBufferPool(session) else { fail("Invalid hardware encoder frame") }
        do {
            var pixel: CVPixelBuffer?
            let limits = [kCVPixelBufferPoolAllocationThresholdKey: 3] as CFDictionary
            try check(CVPixelBufferPoolCreatePixelBufferWithAuxAttributes(kCFAllocatorDefault, pool, limits, &pixel), "allocate bounded encoder surface")
            guard let pixel = pixel else { fail("Could not allocate encoder surface") }
            try check(CVPixelBufferLockBaseAddress(pixel, []), "lock encoder surface")
            let base = CVPixelBufferGetBaseAddress(pixel)!
            let stride = CVPixelBufferGetBytesPerRow(pixel)
            data.withUnsafeBytes { raw in
                for row in 0..<height { base.advanced(by: row * stride).copyMemory(from: raw.baseAddress!.advanced(by: 16 + row * width * 4), byteCount: width * 4) }
            }
            CVPixelBufferUnlockBaseAddress(pixel, [])
            let properties = [kVTEncodeFrameOptionKey_ForceKeyFrame: number(data, 12) == 1] as CFDictionary
            produced = false
            try check(VTCompressionSessionEncodeFrame(session, imageBuffer: pixel, presentationTimeStamp: CMTime(value: sequence, timescale: Int32(fps)),
                duration: CMTime(value: 1, timescale: Int32(fps)), frameProperties: properties, sourceFrameRefcon: nil, infoFlagsOut: nil), "encode hardware frame")
            sequence += 1
            try check(VTCompressionSessionCompleteFrames(session, untilPresentationTimeStamp: .invalid), "complete hardware frame")
            if !produced { fail("Hardware encoder did not produce a frame") }
        } catch { fail(error.localizedDescription) }
    }
    func output(_ status: OSStatus, _ flags: VTEncodeInfoFlags, _ sample: CMSampleBuffer?) {
        guard status == noErr, !flags.contains(.frameDropped), let sample = sample, sample.isValid,
              let format = sample.formatDescription, let buffer = sample.dataBuffer else { fail("Hardware encoder output failed (\(status))") }
        let attachments = CMSampleBufferGetSampleAttachmentsArray(sample, createIfNecessary: false) as? [[CFString: Any]]
        let keyframe = (attachments?.first?[kCMSampleAttachmentKey_NotSync] as? Bool) != true
        var encoded = Data()
        append(keyframe ? 1 : 0, &encoded)
        var pointer: UnsafePointer<UInt8>?, size = 0, count = 0, headerLength: Int32 = 0
        guard CMVideoFormatDescriptionGetH264ParameterSetAtIndex(format, parameterSetIndex: 0, parameterSetPointerOut: &pointer, parameterSetSizeOut: &size, parameterSetCountOut: &count, nalUnitHeaderLengthOut: &headerLength) == noErr,
              headerLength >= 1, headerLength <= 4 else { fail("Invalid H.264 parameter sets") }
        if keyframe {
            for i in 0..<count {
                guard CMVideoFormatDescriptionGetH264ParameterSetAtIndex(format, parameterSetIndex: i, parameterSetPointerOut: &pointer, parameterSetSizeOut: &size, parameterSetCountOut: nil, nalUnitHeaderLengthOut: nil) == noErr,
                      let pointer = pointer, size > 0 else { fail("Invalid H.264 parameter set") }
                encoded.append(contentsOf: [0, 0, 0, 1]); encoded.append(pointer, count: size)
            }
        }
        let length = CMBlockBufferGetDataLength(buffer)
        var avcc = Data(count: length)
        let copied = avcc.withUnsafeMutableBytes { raw in CMBlockBufferCopyDataBytes(buffer, atOffset: 0, dataLength: length, destination: raw.baseAddress!) }
        guard copied == noErr else { fail("Could not read H.264 output") }
        var offset = 0
        while offset < avcc.count {
            guard offset + Int(headerLength) <= avcc.count else { fail("Truncated H.264 length") }
            var size = 0
            for byte in avcc[offset..<offset + Int(headerLength)] { size = (size << 8) | Int(byte) }
            offset += Int(headerLength)
            guard size > 0, size <= avcc.count - offset else { fail("Truncated H.264 NAL") }
            encoded.append(contentsOf: [0, 0, 0, 1]); encoded.append(avcc[offset..<offset + size]); offset += size
        }
        produced = true
        packet(3, encoded)
    }
}

final class Capture: NSObject, SCStreamOutput, SCStreamDelegate {
    var stream: SCStream?
    let input = Input()
    var scale: UInt32 = 1000
    var config: SCStreamConfiguration?
    func resize(_ width: Int, _ height: Int) {
        guard let stream = stream, let config = config else { return }
        let pw = CGDisplayPixelsWide(input.display), ph = CGDisplayPixelsHigh(input.display)
        let ratio = min(1, min(Double(max(2, min(width, 1920))) / Double(pw), Double(max(2, min(height, 1200))) / Double(ph)))
        let w = max(2, Int(Double(pw) * ratio) / 2 * 2), h = max(2, Int(Double(ph) * ratio) / 2 * 2)
        if config.width == w && config.height == h { return }
        config.width = w; config.height = h
        Task { do { try await stream.updateConfiguration(config) } catch { fail("Could not resize capture: \(error.localizedDescription)") } }
    }
    func start(_ requested: String, _ maxWidth: Int, _ maxHeight: Int, _ fps: Int) async {
        guard CGPreflightScreenCaptureAccess() else {
            _ = CGRequestScreenCaptureAccess()
            fail("Screen Recording permission is required. Open Gliff on this Mac and enable it in System Settings > Privacy & Security > Screen & System Audio Recording, then reconnect.")
        }
        guard AXIsProcessTrusted() else {
            fail("Accessibility permission is required to control this Mac. Enable Gliff in System Settings > Privacy & Security > Accessibility, then reconnect.")
        }
        do {
            let content = try await SCShareableContent.excludingDesktopWindows(false, onScreenWindowsOnly: true)
            guard let display = content.displays.first(where: { requested == "auto" ? $0.displayID == CGMainDisplayID() : String($0.displayID) == requested || "display-\($0.displayID)" == requested }) else {
                fail("Display \(requested) was not found. Use gliff-server --outputs to list displays.")
            }
            input.display = display.displayID
            let physicalWidth = CGDisplayPixelsWide(display.displayID)
            let physicalHeight = CGDisplayPixelsHigh(display.displayID)
            let ratio = min(1, min(Double(maxWidth) / Double(physicalWidth), Double(maxHeight) / Double(physicalHeight)))
            let width = max(2, Int(Double(physicalWidth) * ratio) / 2 * 2)
            let height = max(2, Int(Double(physicalHeight) * ratio) / 2 * 2)
            scale = UInt32(max(1, Double(width) / CGDisplayBounds(display.displayID).width * 1000))
            let config = SCStreamConfiguration()
            config.width = width; config.height = height
            config.pixelFormat = kCVPixelFormatType_32BGRA
            config.colorSpaceName = CGColorSpace.sRGB
            config.minimumFrameInterval = CMTime(value: 1, timescale: Int32(fps))
            config.queueDepth = 3; config.showsCursor = true
            self.config = config
            let filter = SCContentFilter(display: display, excludingWindows: [])
            let stream = SCStream(filter: filter, configuration: config, delegate: self)
            try stream.addStreamOutput(self, type: .screen, sampleHandlerQueue: DispatchQueue(label: "gliff.capture", qos: .userInteractive))
            self.stream = stream
            event(["type": "ready", "width": width, "height": height, "scale_milli": scale, "output": "display-\(display.displayID)"])
            try await stream.startCapture()
            DispatchQueue.main.async { clipboard.start() }
        } catch { fail("Screen capture failed: \(error.localizedDescription)") }
    }
    func stream(_ stream: SCStream, didStopWithError error: Error) { fail("Screen capture stopped: \(error.localizedDescription)") }
    func stream(_ stream: SCStream, didOutputSampleBuffer sample: CMSampleBuffer, of type: SCStreamOutputType) {
        autoreleasepool { sendFrame(sample, type) }
    }
    func sendFrame(_ sample: CMSampleBuffer, _ type: SCStreamOutputType) {
        guard type == .screen, sample.isValid, let buffer = sample.imageBuffer else { return }
        // Skip idle/blank samples; ScreenCaptureKit can reuse their previous image.
        if let attachments = CMSampleBufferGetSampleAttachmentsArray(sample, createIfNecessary: false) as? [[SCStreamFrameInfo: Any]], let raw = attachments.first?[.status] as? Int, raw != SCFrameStatus.complete.rawValue { return }
        CVPixelBufferLockBaseAddress(buffer, .readOnly)
        defer { CVPixelBufferUnlockBaseAddress(buffer, .readOnly) }
        guard let base = CVPixelBufferGetBaseAddress(buffer) else { return }
        let width = CVPixelBufferGetWidth(buffer), height = CVPixelBufferGetHeight(buffer)
        let stride = CVPixelBufferGetBytesPerRow(buffer)
        var data = Data(capacity: 12 + width * height * 4)
        let frameScale = UInt32(max(1, Double(width) / CGDisplayBounds(input.display).width * 1000))
        append(UInt32(width), &data); append(UInt32(height), &data); append(frameScale, &data)
        for row in 0..<height { data.append(base.advanced(by: row * stride).assumingMemoryBound(to: UInt8.self), count: width * 4) }
        packet(1, data)
    }
}

final class RemoteView: NSView {
    var image: CGImage? {
        didSet { window?.invalidateCursorRects(for: self) }
    }
    private let invisibleCursor = NSCursor(image: NSImage(size: NSSize(width: 1, height: 1), flipped: false, drawingHandler: { _ in true }), hotSpot: .zero)
    var cursorInVideo = false {
        didSet { window?.invalidateCursorRects(for: self) }
    }
    var scale: Double = 1
    var pressed: Set<UInt16> = []
    var buttons: Set<Int> = []
    var released = false
    var hovering = false
    var message = "Connecting…"
    var tracking: NSTrackingArea?
    override var acceptsFirstResponder: Bool { true }
    override var isFlipped: Bool { true }
    override func updateTrackingAreas() {
        if let old = tracking { removeTrackingArea(old) }
        let area = NSTrackingArea(rect: .zero, options: [.activeInKeyWindow, .inVisibleRect, .mouseMoved, .mouseEnteredAndExited], owner: self)
        addTrackingArea(area); tracking = area
        super.updateTrackingAreas()
    }
    override func resetCursorRects() {
        super.resetCursorRects()
        if image != nil && cursorInVideo { addCursorRect(pictureRect, cursor: invisibleCursor) }
    }
    var pictureRect: NSRect {
        guard let image = image else { return .zero }
        let ratio = min(bounds.width / Double(image.width), bounds.height / Double(image.height))
        let w = Double(image.width) * ratio, h = Double(image.height) * ratio
        return NSRect(x: (bounds.width - w)/2, y: (bounds.height - h)/2, width: w, height: h)
    }
    override func draw(_ dirty: NSRect) {
        NSColor.black.setFill(); bounds.fill()
        if let image = image, let cg = NSGraphicsContext.current?.cgContext {
            cg.saveGState(); cg.interpolationQuality = .none
            let r = pictureRect
            cg.translateBy(x: r.minX, y: r.maxY); cg.scaleBy(x: 1, y: -1)
            cg.draw(image, in: CGRect(x: 0, y: 0, width: r.width, height: r.height)); cg.restoreGState()
        } else {
            let attrs: [NSAttributedString.Key: Any] = [.foregroundColor: NSColor.white, .font: NSFont.systemFont(ofSize: 16)]
            let text = message as NSString
            let size = text.size(withAttributes: attrs)
            text.draw(in: NSRect(x: 24, y: bounds.height/2, width: max(bounds.width-48, size.width), height: 100), withAttributes: attrs)
        }
    }
    func release() {
        for key in pressed { event(["type": "key", "keycode": key, "pressed": false]) }
        for button in buttons { event(["type": "button", "button": button, "pressed": false]) }
        pressed.removeAll(); buttons.removeAll()
    }
    func key(_ e: NSEvent, _ down: Bool) {
        if down && e.keyCode == 53 && e.modifierFlags.contains(.shift) { release(); released = true; return }
        guard hovering, !released, !e.isARepeat else { return }
        if down { pressed.insert(e.keyCode) } else { pressed.remove(e.keyCode) }
        event(["type": "key", "keycode": e.keyCode, "pressed": down])
    }
    override func keyDown(with e: NSEvent) { key(e, true) }
    override func keyUp(with e: NSEvent) { key(e, false) }
    override func flagsChanged(with e: NSEvent) {
        guard hovering, !released else { return }
        // Caps Lock sends flagsChanged on both toggle directions, rather than
        // ordinary key-down/up events. Each transition must toggle the host.
        if e.keyCode == 57 {
            event(["type": "key", "keycode": 57, "pressed": true])
            event(["type": "key", "keycode": 57, "pressed": false])
            return
        }
        let mask: NSEvent.ModifierFlags
        switch e.keyCode {
        case 54, 55: mask = .command
        case 56, 60: mask = .shift
        case 58, 61: mask = .option
        case 59, 62: mask = .control
        default: return
        }
        let down = e.modifierFlags.contains(mask) && !pressed.contains(e.keyCode)
        if down { pressed.insert(e.keyCode) } else { pressed.remove(e.keyCode) }
        event(["type": "key", "keycode": e.keyCode, "pressed": down])
    }
    func motion(_ e: NSEvent) {
        let p = convert(e.locationInWindow, from: nil), r = pictureRect
        guard let image = image, r.contains(p) else {
            if hovering { release() }
            hovering = false
            return
        }
        hovering = true
        event(["type": "motion", "x": (p.x-r.minX)/r.width*Double(image.width)/scale, "y": (p.y-r.minY)/r.height*Double(image.height)/scale])
    }
    override func mouseMoved(with e: NSEvent) { motion(e) }
    override func mouseDragged(with e: NSEvent) { motion(e) }
    override func rightMouseDragged(with e: NSEvent) { motion(e) }
    override func otherMouseDragged(with e: NSEvent) { motion(e) }
    override func mouseEntered(with e: NSEvent) { hovering = pictureRect.contains(convert(e.locationInWindow, from: nil)); released = false }
    override func mouseExited(with e: NSEvent) { hovering = false; release() }
    func button(_ e: NSEvent, _ down: Bool) {
        if down {
            guard image != nil, pictureRect.contains(convert(e.locationInWindow, from: nil)) else {
                motion(e)
                return
            }
            hovering = true; released = false
            window?.makeFirstResponder(self)
            buttons.insert(e.buttonNumber)
        } else if buttons.remove(e.buttonNumber) == nil {
            return
        }
        motion(e); event(["type": "button", "button": e.buttonNumber, "pressed": down])
    }
    override func mouseDown(with e: NSEvent) { button(e, true) }
    override func mouseUp(with e: NSEvent) { button(e, false) }
    override func rightMouseDown(with e: NSEvent) { button(e, true) }
    override func rightMouseUp(with e: NSEvent) { button(e, false) }
    override func otherMouseDown(with e: NSEvent) { button(e, true) }
    override func otherMouseUp(with e: NSEvent) { button(e, false) }
    override func scrollWheel(with e: NSEvent) {
        guard pictureRect.contains(convert(e.locationInWindow, from: nil)) else { return }
        let factor = e.hasPreciseScrollingDeltas ? 1.0 : 10.0
        if e.scrollingDeltaY != 0 { event(["type": "axis", "horizontal": false, "value": -e.scrollingDeltaY * factor]) }
        if e.scrollingDeltaX != 0 { event(["type": "axis", "horizontal": true, "value": -e.scrollingDeltaX * factor]) }
    }
}

final class App: NSObject, NSApplicationDelegate, NSWindowDelegate {
    let view = RemoteView()
    var window: NSWindow?
    var pending: Data?
    let pendingLock = NSLock()
    var drawing = false
    var painted = false
    func applicationDidFinishLaunching(_ note: Notification) {
        let menu = NSMenu(); let appItem = NSMenuItem(); menu.addItem(appItem)
        let appMenu = NSMenu(); appItem.submenu = appMenu
        let hosting = appMenu.addItem(withTitle: "Enable Remote Control…", action: #selector(App.requestHostPermissions), keyEquivalent: "")
        hosting.target = self
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "Quit Gliff", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        NSApplication.shared.mainMenu = menu
        let w = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1100, height: 720), styleMask: [.titled, .closable, .miniaturizable, .resizable], backing: .buffered, defer: false)
        w.title = CommandLine.arguments.dropFirst(2).first ?? "Gliff"
        w.contentView = view; w.delegate = self; w.acceptsMouseMovedEvents = true
        w.center(); w.makeKeyAndOrderFront(nil); w.makeFirstResponder(view); window = w
        NSApplication.shared.activate(ignoringOtherApps: true)
        clipboard.start()
        keyboardLayoutChanged(nil)
        DistributedNotificationCenter.default().addObserver(self, selector: #selector(keyboardLayoutChanged(_:)), name: Notification.Name(kTISNotifySelectedKeyboardInputSourceChanged as String), object: nil)
        resizeEvent()
        NSEvent.addLocalMonitorForEvents(matching: [.keyDown, .keyUp]) { [weak self] e in
            guard let self = self, self.window?.isKeyWindow == true, !self.view.released, self.view.hovering, self.view.image != nil else { return e }
            self.view.key(e, e.type == .keyDown); return nil
        }
        readCommands { [weak self] kind, data in
            guard let self = self else { return }
            if kind == 1 {
                self.pendingLock.lock(); self.pending = data
                let schedule = !self.drawing; self.drawing = true; self.pendingLock.unlock()
                if schedule { DispatchQueue.main.async { autoreleasepool { self.drawPending() } } }
            } else {
                let cmd = json(data)
                if clipboard.receive(cmd) { return }
                DispatchQueue.main.async {
                    clipboard.command(cmd)
                    if cmd["type"] as? String == "cursor_mode" { self.view.cursorInVideo = cmd["embedded"] as? Bool == true }
                    if let message = cmd["message"] as? String { self.view.message = message; self.view.image = nil; self.view.needsDisplay = true }
                    if let text = cmd["clipboard"] as? String { NSPasteboard.general.clearContents(); NSPasteboard.general.setString(text, forType: .string) }
                }
            }
        }
    }
    @objc func requestHostPermissions() {
        _ = CGRequestScreenCaptureAccess()
        _ = AXIsProcessTrustedWithOptions([kAXTrustedCheckOptionPrompt.takeUnretainedValue() as String: true] as CFDictionary)
        if let url = URL(string: "x-apple.systempreferences:com.apple.preference.security?Privacy_ScreenCapture") { NSWorkspace.shared.open(url) }
    }
    func drawPending() {
        pendingLock.lock(); let data = pending; pending = nil; drawing = false; pendingLock.unlock()
        guard let data = data, data.count >= 12 else { return }
        let w = Int(number(data, 0)), h = Int(number(data, 4)), s = number(data, 8)
        guard w > 0, h > 0, s > 0, w * h * 4 == data.count - 12, let provider = CGDataProvider(data: data.subdata(in: 12..<data.count) as CFData) else { return }
        view.image = CGImage(width: w, height: h, bitsPerComponent: 8, bitsPerPixel: 32, bytesPerRow: w * 4, space: CGColorSpace(name: CGColorSpace.sRGB)!, bitmapInfo: CGBitmapInfo(rawValue: CGImageAlphaInfo.noneSkipFirst.rawValue).union(.byteOrder32Little), provider: provider, decode: nil, shouldInterpolate: false, intent: .defaultIntent)
        view.scale = Double(s)/1000; view.needsDisplay = true
        if !painted { painted = true; diagnostic("Gliff displayed first frame: \(w)x\(h)") }
    }
    func resizeEvent() {
        let factor = window?.backingScaleFactor ?? 1
        event(["type": "resize", "width": Int(view.bounds.width * factor), "height": Int(view.bounds.height * factor), "scale": factor])
    }
    @objc func keyboardLayoutChanged(_ note: Notification?) {
        if let layout = keyboardLayout() { event(layout) }
    }
    func windowDidResize(_ note: Notification) { resizeEvent() }
    func windowDidEnterFullScreen(_ note: Notification) { resizeEvent() }
    func windowDidExitFullScreen(_ note: Notification) { resizeEvent() }
    func windowDidChangeBackingProperties(_ note: Notification) { resizeEvent() }
    func windowDidResignKey(_ note: Notification) { view.release() }
    func applicationShouldTerminateAfterLastWindowClosed(_ app: NSApplication) -> Bool { true }
    func applicationWillTerminate(_ note: Notification) { DistributedNotificationCenter.default().removeObserver(self); view.release(); event(["type": "bye"]) }
}

struct NearbyComputer {
    let id: String
    let name: String
    let host: String
    let port: Int
    let user: String
    let platform: String
    var address: String { user.isEmpty ? host : "\(user)@\(host)" }
    var displayName: String {
        let hostname = host.hasSuffix(".local") ? String(host.dropLast(6)) : host
        return name == "Gliff on \(hostname) (\(user))" ? hostname : name
    }
    var value: [String: Any] { ["name": name, "host": host, "port": port, "user": user, "platform": platform] }
}

// Bonjour supplies additions/removals rather than repeated subnet scans.
// Keep at most 128 service objects and resolve names only, not SSH connections.
final class NearbyComputers: NSObject, NetServiceBrowserDelegate, NetServiceDelegate {
    let browser = NetServiceBrowser()
    var services: [String: NetService] = [:]
    var records: [String: NearbyComputer] = [:]
    var onChange: (() -> Void)?
    var failure: String?
    var computers: [NearbyComputer] {
        records.values.sorted { ($0.name, $0.address) < ($1.name, $1.address) }
    }
    func key(_ service: NetService) -> String { "\(service.name)\n\(service.type)\n\(service.domain)" }
    func start() {
        stop(); failure = nil
        browser.delegate = self
        browser.searchForServices(ofType: "_gliff._tcp.", inDomain: "local.")
        onChange?()
    }
    func stop() {
        browser.stop()
        for service in services.values { service.stopMonitoring(); service.stop(); service.delegate = nil }
        services.removeAll(); records.removeAll()
    }
    func netServiceBrowser(_ browser: NetServiceBrowser, didFind service: NetService, moreComing: Bool) {
        let id = key(service)
        guard services[id] == nil, services.count < 128 else { return }
        services[id] = service; service.delegate = self
        service.resolve(withTimeout: 5)
    }
    func netServiceBrowser(_ browser: NetServiceBrowser, didRemove service: NetService, moreComing: Bool) {
        let id = key(service)
        if let old = services.removeValue(forKey: id) {
            old.stopMonitoring(); old.stop(); old.delegate = nil
        }
        records.removeValue(forKey: id)
        onChange?()
    }
    func netServiceBrowser(_ browser: NetServiceBrowser, didNotSearch errorDict: [String: NSNumber]) {
        failure = "Nearby computers unavailable. Allow Gliff in System Settings → Privacy & Security → Local Network, or enter an SSH address."
        onChange?()
    }
    func netServiceDidResolveAddress(_ service: NetService) {
        update(service); service.startMonitoring()
    }
    func netService(_ service: NetService, didUpdateTXTRecord data: Data) { update(service) }
    func netService(_ service: NetService, didNotResolve errorDict: [String: NSNumber]) {
        records.removeValue(forKey: key(service)); onChange?()
    }
    func update(_ service: NetService) {
        let id = key(service)
        guard services[id] === service else { return }
        let txt = NetService.dictionary(fromTXTRecord: service.txtRecordData() ?? Data())
        func text(_ key: String) -> String { txt[key].flatMap { String(data: $0, encoding: .utf8) } ?? "" }
        let host = (service.hostName ?? "").trimmingCharacters(in: CharacterSet(charactersIn: "."))
        guard text("transport") == "ssh", text("protocol") == "1", (1...65535).contains(service.port),
              host.range(of: "^[A-Za-z0-9_][A-Za-z0-9_.-]{0,252}$", options: .regularExpression) != nil else {
            records.removeValue(forKey: id); onChange?(); return
        }
        let hint = text("user")
        let user = hint.range(of: "^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$", options: .regularExpression) != nil ? hint : ""
        records[id] = NearbyComputer(id: id, name: service.name, host: host, port: service.port, user: user, platform: text("platform"))
        onChange?()
    }
}

final class ComputerPickerRow: NSTableRowView {
    override func drawSelection(in dirtyRect: NSRect) {
        guard selectionHighlightStyle != .none else { return }
        let color = isEmphasized ? NSColor.selectedContentBackgroundColor : NSColor.unemphasizedSelectedContentBackgroundColor
        color.setFill()
        var rect = bounds.insetBy(dx: 4, dy: 3)
        if let scroll = enclosingScrollView { rect.size.width = min(bounds.width, scroll.contentSize.width) - 8 }
        NSBezierPath(roundedRect: rect, xRadius: 6, yRadius: 6).fill()
    }
}

final class ComputerPicker: NSObject, NSApplicationDelegate, NSWindowDelegate, NSTableViewDataSource, NSTableViewDelegate, NSTextFieldDelegate {
    let nearby = NearbyComputers()
    let table = NSTableView()
    let address = NSTextField(string: "")
    let port = NSTextField(string: "22")
    let status = NSTextField(wrappingLabelWithString: "Searching the local network…")
    let progress = NSProgressIndicator()
    let screen = NSPopUpButton()
    let screenDetail = NSTextField(wrappingLabelWithString: "")
    let preferences: UserDefaults
    let screenOverride: String?
    var preferenceTarget: String?
    var window: NSWindow?
    var listHeight: NSLayoutConstraint?
    var rows: [NearbyComputer] = []
    var updating = false
    var finished = false
    var selectedID: String?
    init(screenOverride: String? = nil, preferences: UserDefaults? = nil) {
        self.screenOverride = screenOverride
        if let preferences = preferences { self.preferences = preferences }
        else if Bundle.main.bundleIdentifier == "org.omacom.gliff" { self.preferences = .standard }
        else { self.preferences = UserDefaults(suiteName: "org.omacom.gliff") ?? .standard }
        super.init()
    }
    func label(_ text: String, _ size: CGFloat = 13) -> NSTextField {
        let label = NSTextField(labelWithString: text)
        label.font = NSFont.systemFont(ofSize: size); return label
    }
    func stack(_ views: [NSView], _ orientation: NSUserInterfaceLayoutOrientation, _ spacing: CGFloat) -> NSStackView {
        let stack = NSStackView(views: views)
        stack.orientation = orientation; stack.spacing = spacing
        stack.alignment = orientation == .vertical ? .leading : .centerY
        stack.translatesAutoresizingMaskIntoConstraints = false
        return stack
    }
    func applicationDidFinishLaunching(_ note: Notification) {
        let menu = NSMenu(); let item = NSMenuItem(); menu.addItem(item)
        let appMenu = NSMenu(); item.submenu = appMenu
        let permission = appMenu.addItem(withTitle: "Enable Remote Control…", action: #selector(hostPermissions), keyEquivalent: "")
        permission.target = self
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "Quit Gliff", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        NSApplication.shared.mainMenu = menu
        let w = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 600, height: 340), styleMask: [.titled, .closable, .miniaturizable, .resizable], backing: .buffered, defer: false)
        w.title = "Gliff"; w.delegate = self
        w.backgroundColor = .windowBackgroundColor
        w.titlebarAppearsTransparent = true
        let view = w.contentView!
        let title = label("Connect to a Computer", 18)
        title.font = NSFont.systemFont(ofSize: 18, weight: .semibold)
        let subtitle = label("Choose a nearby computer or enter an SSH address.")
        subtitle.textColor = .secondaryLabelColor
        let heading = stack([title, subtitle], .vertical, 4)
        let list = NSBox()
        list.boxType = .custom; list.titlePosition = .noTitle
        list.fillColor = .controlBackgroundColor; list.borderColor = .separatorColor
        list.borderWidth = 0.5; list.cornerRadius = 8; list.contentViewMargins = .zero
        list.wantsLayer = true; list.layer?.cornerRadius = 8; list.layer?.masksToBounds = true
        let scroll = NSScrollView()
        scroll.translatesAutoresizingMaskIntoConstraints = false
        scroll.hasVerticalScroller = true; scroll.borderType = .noBorder
        for (id, title, width) in [("name", "Computer", CGFloat(180)), ("address", "SSH Address", CGFloat(324))] {
            let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier(id)); column.title = title; column.width = width
            table.addTableColumn(column)
        }
        table.delegate = self; table.dataSource = self; table.rowHeight = 30
        table.style = .inset; table.usesAlternatingRowBackgroundColors = true
        table.columnAutoresizingStyle = .lastColumnOnlyAutoresizingStyle
        table.setAccessibilityLabel("Nearby Computers")
        table.frame = NSRect(x: 0, y: 0, width: 536, height: 66); table.autoresizingMask = [.width]
        table.target = self; table.doubleAction = #selector(connect)
        scroll.documentView = table; list.contentView!.addSubview(scroll)
        progress.style = .spinning; progress.controlSize = .small
        progress.isIndeterminate = true; progress.isDisplayedWhenStopped = false
        progress.setAccessibilityLabel("Searching for nearby computers")
        status.font = NSFont.systemFont(ofSize: 12); status.textColor = .secondaryLabelColor
        status.preferredMaxLayoutWidth = 496
        status.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let refresh = NSButton(image: NSImage(systemSymbolName: "arrow.clockwise", accessibilityDescription: "Refresh Nearby Computers")!, target: self, action: #selector(refresh))
        refresh.bezelStyle = .inline; refresh.controlSize = .small
        refresh.toolTip = "Refresh Nearby Computers"; refresh.setAccessibilityLabel("Refresh Nearby Computers")
        let statusRow = stack([progress, status, refresh], .horizontal, 8)
        address.placeholderString = "user@computer.local"
        address.bezelStyle = .roundedBezel; port.bezelStyle = .roundedBezel
        address.font = NSFont.systemFont(ofSize: 13); port.font = address.font
        address.setAccessibilityLabel("SSH Address"); port.setAccessibilityLabel("SSH Port")
        address.setContentHuggingPriority(.defaultLow, for: .horizontal)
        address.delegate = self; port.delegate = self
        let fields = stack([label("SSH Address:"), address, label("Port:"), port], .horizontal, 10)
        screen.addItems(withTitles: ["Mirror screen", "Private screen"])
        screen.setAccessibilityLabel("Screen mode")
        screen.target = self; screen.action = #selector(screenChanged)
        screen.isEnabled = screenOverride == nil
        screenDetail.font = NSFont.systemFont(ofSize: 12); screenDetail.textColor = .secondaryLabelColor
        let screenRow = stack([label("Screen:"), screen, screenDetail], .horizontal, 10)
        screenDetail.setContentHuggingPriority(.defaultLow, for: .horizontal)
        screenDetail.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
        let cancel = NSButton(title: "Cancel", target: self, action: #selector(cancel))
        cancel.bezelStyle = .rounded; cancel.controlSize = .large; cancel.keyEquivalent = "\u{1b}"
        let connect = NSButton(title: "Connect", target: self, action: #selector(connect))
        connect.bezelStyle = .rounded; connect.controlSize = .large; connect.keyEquivalent = "\r"
        let actions = stack([NSView(), cancel, connect], .horizontal, 8)
        let content = stack([heading, list, statusRow, fields, screenRow, actions], .vertical, 12)
        view.addSubview(content)
        for row in [heading, list, statusRow, fields, screenRow, actions] { row.translatesAutoresizingMaskIntoConstraints = false }
        let listHeight = list.heightAnchor.constraint(equalToConstant: 66)
        self.listHeight = listHeight
        NSLayoutConstraint.activate([
            view.widthAnchor.constraint(greaterThanOrEqualToConstant: 600),
            content.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 20),
            content.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -20),
            content.topAnchor.constraint(equalTo: view.topAnchor, constant: 20),
            content.bottomAnchor.constraint(equalTo: view.bottomAnchor, constant: -20),
            list.widthAnchor.constraint(equalTo: content.widthAnchor),
            listHeight,
            scroll.leadingAnchor.constraint(equalTo: list.contentView!.leadingAnchor),
            scroll.trailingAnchor.constraint(equalTo: list.contentView!.trailingAnchor),
            scroll.topAnchor.constraint(equalTo: list.contentView!.topAnchor),
            scroll.bottomAnchor.constraint(equalTo: list.contentView!.bottomAnchor),
            statusRow.widthAnchor.constraint(equalTo: content.widthAnchor),
            statusRow.heightAnchor.constraint(greaterThanOrEqualToConstant: 20),
            progress.widthAnchor.constraint(equalToConstant: 16),
            progress.heightAnchor.constraint(equalToConstant: 16),
            fields.widthAnchor.constraint(equalTo: content.widthAnchor),
            port.widthAnchor.constraint(equalToConstant: 64),
            screenRow.widthAnchor.constraint(equalTo: content.widthAnchor),
            screen.widthAnchor.constraint(equalToConstant: 140),
            actions.widthAnchor.constraint(equalTo: content.widthAnchor),
            cancel.widthAnchor.constraint(greaterThanOrEqualToConstant: 80),
            connect.widthAnchor.constraint(greaterThanOrEqualToConstant: 90),
        ])
        table.nextKeyView = address; address.nextKeyView = port
        port.nextKeyView = screen; screen.nextKeyView = connect; connect.nextKeyView = cancel; cancel.nextKeyView = refresh
        refresh.nextKeyView = table
        w.defaultButtonCell = connect.cell as? NSButtonCell
        window = w; restoreScreenPreference(); fitWindow()
        w.center(); w.makeKeyAndOrderFront(nil); w.makeFirstResponder(address)
        NSApplication.shared.activate(ignoringOtherApps: true)
        nearby.onChange = { [weak self] in self?.update() }
        nearby.start()
        // The parent can close/inspect this chooser without making a connection.
        readCommands(onClose: { [weak self] in self?.cancel(nil) }) { [weak self] _, data in
            let cmd = json(data)
            DispatchQueue.main.async {
                guard let self = self else { return }
                if cmd["type"] as? String == "cancel" { self.cancel(nil) }
                if cmd["type"] as? String == "select", let index = cmd["index"] as? Int, self.rows.indices.contains(index) {
                    self.table.selectRowIndexes(IndexSet(integer: index), byExtendingSelection: false)
                    self.window?.makeFirstResponder(self.table)
                }
                if cmd["type"] as? String == "inspect" {
                    event(["type": "picker_state", "hosts": self.rows.map { $0.value }, "rows": self.table.numberOfRows, "host": self.address.stringValue, "port": self.port.stringValue, "screen": self.screenMode])
                }
            }
        }
    }
    func update() {
        updating = true; rows = nearby.computers; table.reloadData()
        if let index = rows.firstIndex(where: { $0.id == selectedID }) {
            table.selectRowIndexes(IndexSet(integer: index), byExtendingSelection: false)
        }
        updating = false
        if rows.isEmpty && nearby.failure == nil { progress.startAnimation(nil) } else { progress.stopAnimation(nil) }
        status.stringValue = nearby.failure ?? (rows.isEmpty ? "Searching for nearby computers… You can also enter an SSH address." : "\(rows.count) computer\(rows.count == 1 ? "" : "s") found. SSH access is required to connect.")
        listHeight?.constant = (table.headerView?.frame.height ?? 22) + CGFloat(max(1, min(rows.count, 4))) * (table.rowHeight + table.intercellSpacing.height) + 12
        fitWindow()
    }
    func fitWindow() {
        guard let window = window, let view = window.contentView else { return }
        // Follow the content until the user enlarges the window; preserve their size afterwards.
        let wasCompact = window.contentMinSize == .zero || window.contentLayoutRect.size == window.contentMinSize
        view.layoutSubtreeIfNeeded()
        let size = view.fittingSize
        window.contentMinSize = size
        if wasCompact { window.setContentSize(size) }
    }
    func numberOfRows(in tableView: NSTableView) -> Int { rows.count }
    func tableView(_ tableView: NSTableView, rowViewForRow row: Int) -> NSTableRowView? { ComputerPickerRow() }
    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        guard rows.indices.contains(row) else { return nil }
        let computer = rows[row]
        let identifier = tableColumn!.identifier
        if let cell = tableView.makeView(withIdentifier: identifier, owner: self) as? NSTableCellView {
            cell.textField?.stringValue = identifier.rawValue == "name" ? computer.displayName : computer.address
            return cell
        }
        let cell = NSTableCellView(); cell.identifier = identifier
        let text = label(identifier.rawValue == "name" ? computer.displayName : computer.address)
        text.lineBreakMode = .byTruncatingTail; text.translatesAutoresizingMaskIntoConstraints = false
        cell.textField = text; cell.addSubview(text)
        var leading = cell.leadingAnchor
        if identifier.rawValue == "name" {
            let icon = NSImageView()
            icon.image = NSImage(systemSymbolName: "desktopcomputer", accessibilityDescription: nil)
            icon.translatesAutoresizingMaskIntoConstraints = false
            cell.imageView = icon; cell.addSubview(icon)
            NSLayoutConstraint.activate([
                icon.leadingAnchor.constraint(equalTo: cell.leadingAnchor, constant: 4),
                icon.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
                icon.widthAnchor.constraint(equalToConstant: 18), icon.heightAnchor.constraint(equalToConstant: 18),
            ])
            leading = icon.trailingAnchor
        }
        NSLayoutConstraint.activate([
            text.leadingAnchor.constraint(equalTo: leading, constant: 8),
            text.trailingAnchor.constraint(equalTo: cell.trailingAnchor, constant: -6),
            text.centerYAnchor.constraint(equalTo: cell.centerYAnchor),
        ])
        return cell
    }
    func tableViewSelectionDidChange(_ notification: Notification) {
        guard !updating, rows.indices.contains(table.selectedRow) else { return }
        let computer = rows[table.selectedRow]; selectedID = computer.id
        address.stringValue = computer.address; port.stringValue = String(computer.port)
        restoreScreenPreference()
    }
    var screenMode: String { screen.indexOfSelectedItem == 1 ? "private" : "mirror" }
    var screenPreferenceKey: String? {
        let host = address.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !host.isEmpty, let number = Int(port.stringValue), (1...65535).contains(number) else { return nil }
        return "screen.\(host):\(number)"
    }
    func restoreScreenPreference() {
        let key = screenPreferenceKey
        guard key != preferenceTarget || preferenceTarget == nil else { return }
        preferenceTarget = key
        let mode = screenOverride ?? key.flatMap { preferences.string(forKey: $0) } ?? "mirror"
        screen.selectItem(at: mode == "private" ? 1 : 0)
        updateScreenDetail()
    }
    func updateScreenDetail() {
        screenDetail.stringValue = screenMode == "private" ? "A separate Linux desktop that follows this window." : "Shows the host desktop, keeping its proportions."
        fitWindow()
    }
    @objc func screenChanged(_ sender: Any?) {
        if let key = screenPreferenceKey { preferences.set(screenMode, forKey: key) }
        updateScreenDetail()
    }
    func controlTextDidChange(_ note: Notification) { restoreScreenPreference() }
    @objc func refresh(_ sender: Any?) { nearby.start() }
    func control(_ control: NSControl, textView: NSTextView, doCommandBy commandSelector: Selector) -> Bool {
        if commandSelector == #selector(NSResponder.insertNewline(_:)) { connect(nil); return true }
        if commandSelector == #selector(NSResponder.cancelOperation(_:)) { cancel(nil); return true }
        return false
    }
    @objc func connect(_ sender: Any?) {
        let host = address.stringValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !host.isEmpty, !host.hasPrefix("-"), host.utf8.count <= 320,
              host.range(of: "^[A-Za-z0-9_.:@\\[\\]%-]+$", options: .regularExpression) != nil,
              let number = Int(port.stringValue), (1...65535).contains(number) else {
            status.stringValue = "Enter an SSH address and a port between 1 and 65535."; fitWindow(); return
        }
        if let key = screenPreferenceKey { preferences.set(screenMode, forKey: key) }
        complete(["type": "connect", "host": host, "port": number, "screen": screenMode])
    }
    @objc func cancel(_ sender: Any?) { complete(["type": "cancel"]) }
    func complete(_ value: [String: Any]) {
        guard !finished else { return }
        finished = true; nearby.stop(); event(value); NSApplication.shared.terminate(nil)
    }
    @objc func hostPermissions(_ sender: Any?) {
        _ = CGRequestScreenCaptureAccess()
        _ = AXIsProcessTrustedWithOptions([kAXTrustedCheckOptionPrompt.takeUnretainedValue() as String: true] as CFDictionary)
        if let url = URL(string: "x-apple.systempreferences:com.apple.preference.security?Privacy_ScreenCapture") { NSWorkspace.shared.open(url) }
    }
    func windowWillClose(_ note: Notification) { cancel(nil) }
    func applicationShouldTerminateAfterLastWindowClosed(_ app: NSApplication) -> Bool { true }
    func applicationWillTerminate(_ note: Notification) {
        if !finished { finished = true; nearby.stop(); event(["type": "cancel"]) }
    }
}

let mode = ProcessInfo.processInfo.environment["GLIFF_ASKPASS"] == "1" ? "askpass" : (CommandLine.arguments.dropFirst().first ?? "permissions")
let app = NSApplication.shared
if mode == "encode" {
    let args = Array(CommandLine.arguments.dropFirst(2))
    guard args.count == 4, let width = Int(args[0]), let height = Int(args[1]), let bitrate = Int(args[2]), let fps = Int(args[3]) else { fail("Invalid encoder configuration") }
    do {
        let encoder = try HardwareEncoder(width, height, bitrate, fps)
        event(["type": "encoder", "hardware": true, "name": "videotoolbox"])
        app.setActivationPolicy(.prohibited)
        readCommands(onClose: { encoder.close() }) { kind, data in
            guard kind == 4 else { fail("Unexpected encoder command") }
            encoder.encode(data)
        }
        withExtendedLifetime(encoder) { app.run() }
    } catch { fail(error.localizedDescription) }
} else if mode == "askpass" {
    app.setActivationPolicy(.accessory); app.activate(ignoringOtherApps: true)
    let prompt = CommandLine.arguments.dropFirst().joined(separator: " ")
    let alert = NSAlert(); alert.messageText = "SSH Authentication"; alert.informativeText = prompt
    if prompt.contains("yes/no") {
        alert.messageText = "Connect to this computer?"
        alert.addButton(withTitle: "Connect"); alert.addButton(withTitle: "Cancel")
        guard alert.runModal() == .alertFirstButtonReturn else { exit(1) }
        print("yes")
    } else {
        let field = NSSecureTextField(frame: NSRect(x: 0, y: 0, width: 340, height: 26))
        alert.accessoryView = field
        alert.addButton(withTitle: "Continue"); alert.addButton(withTitle: "Cancel")
        alert.window.initialFirstResponder = field
        guard alert.runModal() == .alertFirstButtonReturn else { exit(1) }
        print(field.stringValue)
    }
    exit(0)
} else if mode == "choose" {
    app.setActivationPolicy(.regular)
    let picker = ComputerPicker(screenOverride: CommandLine.arguments.dropFirst(2).first); app.delegate = picker
    withExtendedLifetime(picker) { app.run() }
} else if mode == "keyboard-layout" {
    guard let layout = keyboardLayout(CommandLine.arguments.dropFirst(2).first) else { fail("Cannot read the Mac Unicode keyboard layout") }
    event(layout)
} else if mode == "discover" {
    app.setActivationPolicy(.accessory)
    let nearby = NearbyComputers(); nearby.start()
    Timer.scheduledTimer(withTimeInterval: 8, repeats: false) { _ in
        if let failure = nearby.failure { event(["type": "error", "message": failure]) }
        else { event(["type": "hosts", "hosts": nearby.computers.map { $0.value }]) }
        nearby.stop(); app.terminate(nil)
    }
    withExtendedLifetime(nearby) { app.run() }
} else if mode == "app" {
    app.setActivationPolicy(.regular)
    let delegate = App(); app.delegate = delegate
    withExtendedLifetime(delegate) { app.run() }
} else if mode == "capture" {
    app.setActivationPolicy(.accessory)
    let args = Array(CommandLine.arguments.dropFirst(2))
    let capture = Capture()
    readCommands(onClose: { capture.input.handle(["type": "release"]) }) { _, data in
        let cmd = json(data)
        if clipboard.receive(cmd) { return }
        DispatchQueue.main.async {
            if cmd["type"] as? String == "resize" { capture.resize(cmd["width"] as? Int ?? 1920, cmd["height"] as? Int ?? 1200) }
            else { capture.input.handle(cmd); clipboard.command(cmd) }
        }
    }
    Task { await capture.start(args.first ?? "auto", Int(args.count > 1 ? args[1] : "1920") ?? 1920, Int(args.count > 2 ? args[2] : "1200") ?? 1200, Int(args.count > 3 ? args[3] : "30") ?? 30) }
    app.run()
    capture.input.handle(["type": "release"])
} else if mode == "outputs" {
    var displays = [CGDirectDisplayID](repeating: 0, count: 32), count: UInt32 = 0
    CGGetActiveDisplayList(32, &displays, &count)
    for id in displays.prefix(Int(count)) {
        let b = CGDisplayBounds(id)
        print("display-\(id)\t\(CGDisplayPixelsWide(id))x\(CGDisplayPixelsHigh(id))\t\(Int(b.width))x\(Int(b.height)) logical\(id == CGMainDisplayID() ? "\tmain" : "")")
    }
} else {
    if CommandLine.arguments.contains("--request") {
        _ = CGRequestScreenCaptureAccess()
        _ = AXIsProcessTrustedWithOptions([kAXTrustedCheckOptionPrompt.takeUnretainedValue() as String: true] as CFDictionary)
    }
    print("Screen Recording: \(CGPreflightScreenCaptureAccess() ? "granted" : "required")")
    print("Accessibility: \(AXIsProcessTrusted() ? "granted" : "required")")
    if !CGPreflightScreenCaptureAccess() || !AXIsProcessTrusted() { exit(1) }
}
