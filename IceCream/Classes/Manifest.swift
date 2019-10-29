//
//  LogConfig.swift
//  IceCream
//
//  Created by 蔡越 on 30/01/2018.
//

import Foundation
import os.log

fileprivate extension OSLog {
    static let iceCream = OSLog(subsystem: "com.unifiedsense.icecream", category: "IceCreamTrace")
}

/// This file is for setting some develop configs for IceCream framework.

public class IceCream {
    
    public static let shared = IceCream()
    
    /// There are quite a lot `print`s in the IceCream source files.
    /// If you don't want to see them in your console, just set `enableLogging` property to false.
    /// The default value is true.
    public var enableLogging: Bool = true
}

internal func log_info (_ message: StaticString, _ args: CVarArg...) {
    log(type: .info, message, args)
}

internal func log_debug (_ message: StaticString, _ args: CVarArg...) {
    log(type: .debug, message, args)
}

internal func log_error (_ message: StaticString, _ args: CVarArg...) {
    log(type: .error, message, args)
}

internal func log_fault (_ message: StaticString, _ args: CVarArg...) {
    log(type: .fault, message, args)
}

internal func log(type: OSLogType, _ message: StaticString, _ args: CVarArg...) {
    if (IceCream.shared.enableLogging) {
        #if DEBUG
        os_log(message, log: OSLog.iceCream, type: type, args)
        #endif
    }
}
