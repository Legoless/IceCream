//
//  PublicDatabaseManager.swift
//  IceCream
//
//  Created by caiyue on 2019/4/22.
//

#if os(macOS)
import Cocoa
#else
import UIKit
#endif

import CloudKit

final class PublicDatabaseManager: DatabaseManager {
    
    let container: CKContainer
    let database: CKDatabase
    
    let syncObjects: [Syncable]
    let settings: SyncSettings
    
    public init(objects: [Syncable], settings: SyncSettings) {
        self.syncObjects = objects
        self.settings = settings
        self.container = settings.container
        self.database = settings.container.publicCloudDatabase
    }
    
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?) {
        guard settings.direction != .upstream else {
            callback?(nil)
            return
        }
        
        // Call callback only once.
        
        let syncObjectsCounter = Atomic(syncObjects.count)
        var errors : [Error] = []
        
        syncObjects.forEach { [weak self] syncObject in
            let predicate = NSPredicate(value: true)
            let query = CKQuery(recordType: syncObject.recordType, predicate: predicate)
            let queryOperation = CKQueryOperation(query: query)
            self?.executeQueryOperation(queryOperation: queryOperation, on: syncObject) { error in
                if let error = error {
                    errors.append(error)
                }
                
                syncObjectsCounter.value -= 1
                
                if syncObjectsCounter.value <= 0 {
                    if let finalError = errors.first {
                        NotificationCenter.default.post(name: Notifications.cloudKitDataPullFailed.name, object: finalError)
                    }
                    else {
                        NotificationCenter.default.post(name: Notifications.cloudKitDataPullCompleted.name, object: nil)
                    }
                    
                    callback?(errors.first)
                }
                else {
                    NotificationCenter.default.post(name: Notifications.cloudKitDataPartialPullCompleted.name, object: self, userInfo: [IceCreamKey.syncableKey.value : syncObject])
                }
            }
        }
    }
    
    func createCustomZonesIfAllowed() {
        
    }
    
    func createDatabaseSubscriptionIfHaveNot() {
        guard settings.direction != .downstream else {
            return
        }
        syncObjects.forEach { createSubscriptionInPublicDatabase(on: $0) }
    }
    
    func startObservingTermination() {
        #if os(iOS) || os(tvOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: UIApplication.willTerminateNotification, object: nil)
        
        #elseif os(macOS)
        
        NotificationCenter.default.addObserver(self, selector: #selector(self.cleanUp), name: NSApplication.willTerminateNotification, object: nil)
        
        #endif
    }
    
    func registerLocalDatabase() {
        guard settings.direction != .downstream else {
            return
        }
        
        syncObjects.forEach { object in
            DispatchQueue.main.async {
                object.registerLocalDatabase()
            }
        }
    }
    
    // MARK: - Private Methods
    private func createSubscriptionInPublicDatabase(on syncObject: Syncable) {
        #if os(iOS) || os(tvOS) || os(macOS)
        let predict = NSPredicate(value: true)
        let subscription = CKQuerySubscription(recordType: syncObject.recordType, predicate: predict, subscriptionID: IceCreamSubscription.cloudKitPublicDatabaseSubscriptionID.id, options: [CKQuerySubscription.Options.firesOnRecordCreation, CKQuerySubscription.Options.firesOnRecordUpdate, CKQuerySubscription.Options.firesOnRecordDeletion])
        
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true // Silent Push
        
        subscription.notificationInfo = notificationInfo
        
        let createOp = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
        createOp.modifySubscriptionsCompletionBlock = { _, _, _ in
            
        }
        createOp.qualityOfService = .utility
        database.add(createOp)
        #endif
    }
    
    @objc func cleanUp() {
        for syncObject in syncObjects {
            syncObject.cleanUp()
        }
    }
}
