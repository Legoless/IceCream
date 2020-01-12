//
//  PrivateZoneDatabaseManager.swift
//  IceCream
//
//  Created by Dal Rupnik on 10/30/19.
//  Copyright © 2019 Unified Sense. All rights reserved.
//

#if os(macOS)
import Cocoa
#else
import UIKit
#endif

import CloudKit

final class PrivateZoneDatabaseManager: DatabaseManager {
    
    let container: CKContainer
    let database: CKDatabase
    
    let syncObjects: [Syncable]
    let settings: SyncSettings
    
    public init(objects: [Syncable], settings: SyncSettings) {
        self.syncObjects = objects
        self.settings = settings
        self.container = settings.container
        self.database = settings.container.privateCloudDatabase
    }
    
    private func fetchSequentialChangesInDatabase(for syncObject : Syncable, callback: ((Error?) -> Void)?) {
        let predicate = NSPredicate(value: true)
        let query = CKQuery(recordType: syncObject.recordType, predicate: predicate)
        let queryOperation = CKQueryOperation(query: query)
        self.executeQueryOperation(queryOperation: queryOperation, on: syncObject) { error in
            if let error = error {
                callback?(error)
            }
            else if let index = self.syncObjects.firstIndex(where: { $0 === syncObject }), index == self.syncObjects.count - 1 {
                callback?(nil)
            }
            else if let index = self.syncObjects.firstIndex(where: { $0 === syncObject }) {
                self.fetchSequentialChangesInDatabase(for: self.syncObjects[index + 1], callback: callback)
            }
        }
    }
    
    
    func fetchChangesInDatabase(_ callback: ((Error?) -> Void)?) {
        guard settings.direction != .upstream else {
            callback?(nil)
            return
        }
        
        if settings.zoneId == CKRecordZone.default().zoneID {
            
            // Call callback only once.
            
            let syncObjectsCounter = Atomic(syncObjects.count)
            var errors : [Error] = []
            
            if settings.sequential {
                fetchSequentialChangesInDatabase(for: syncObjects.first!) { error in
                    if let error = error {
                        NotificationCenter.default.post(name: Notifications.cloudKitDataPullFailed.name, object: error)
                    }
                    else {
                        NotificationCenter.default.post(name: Notifications.cloudKitDataPullCompleted.name, object: nil)
                    }
                    
                    callback?(error)
                }
            }
            else {
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
                            NotificationCenter.default.post(name: Notifications.cloudKitDataPartialPullCompleted.name, object: self, userInfo: [IceCreamKey.syncableKey : syncObject])
                        }
                    }
                }
            }
        }
        else {
            let changesOperation = CKFetchDatabaseChangesOperation(previousServerChangeToken: databaseChangeToken)
            
            /// Only update the changeToken when fetch process completes
            changesOperation.changeTokenUpdatedBlock = { [weak self] newToken in
                self?.databaseChangeToken = newToken
            }
            
            changesOperation.fetchDatabaseChangesCompletionBlock = {
                [weak self]
                newToken, _, error in
                guard let self = self else { return }
                switch ErrorHandler.shared.resultType(with: error) {
                case .success:
                    self.databaseChangeToken = newToken
                    // Fetch the changes in zone level
                    self.fetchChangesInZone(callback)
                case .retry(let timeToWait, _):
                    ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                        self.fetchChangesInDatabase(callback)
                    })
                case .recoverableError(let reason, _):
                    switch reason {
                    case .changeTokenExpired:
                        /// The previousServerChangeToken value is too old and the client must re-sync from scratch
                        self.databaseChangeToken = nil
                        self.fetchChangesInDatabase(callback)
                    default:
                        return
                    }
                default:
                    return
                }
            }
            
            database.add(changesOperation)
        }
    }

    func createCustomZonesIfAllowed() {
        guard settings.direction != .downstream else {
            return
        }
        let zonesToCreate = [ CKRecordZone(zoneID: settings.zoneId) ].filter { !$0.isCreated }
        guard zonesToCreate.count > 0 else { return }
        
        let modifyOp = CKModifyRecordZonesOperation(recordZonesToSave: zonesToCreate, recordZoneIDsToDelete: nil)
        modifyOp.modifyRecordZonesCompletionBlock = { [weak self](_, _, error) in
            guard let self = self else { return }
            switch ErrorHandler.shared.resultType(with: error) {
            case .success:
                zonesToCreate.forEach { zone in
                    zone.isCreated = true
                }
                self.syncObjects.forEach { object in
                    
                    // As we register local database in the first step, we have to force push local objects which
                    // have not been caught to CloudKit to make data in sync
                    DispatchQueue.main.async {
                        object.pushLocalObjectsToCloudKit()
                    }
                }
            case .retry(let timeToWait, _):
                ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                    self.createCustomZonesIfAllowed()
                })
            default:
                return
            }
        }
        
        database.add(modifyOp)
    }
    
    func createDatabaseSubscriptionIfHaveNot() {
        #if os(iOS) || os(tvOS) || os(macOS)
        guard !subscriptionIsLocallyCached else { return }
        let subscription = CKDatabaseSubscription(subscriptionID: IceCreamSubscription.cloudKitPrivateDatabaseSubscriptionID.id)
        
        let notificationInfo = CKSubscription.NotificationInfo()
        notificationInfo.shouldSendContentAvailable = true // Silent Push
        
        subscription.notificationInfo = notificationInfo
        
        let createOp = CKModifySubscriptionsOperation(subscriptionsToSave: [subscription], subscriptionIDsToDelete: [])
        createOp.modifySubscriptionsCompletionBlock = { _, _, error in
            guard error == nil else { return }
            self.subscriptionIsLocallyCached = true
        }
        createOp.qualityOfService = .utility
        database.add(createOp)
        #endif
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
        self.syncObjects.forEach { object in
            DispatchQueue.main.async {
                object.registerLocalDatabase()
            }
        }
    }
    
    private func fetchChangesInZone(_ callback: ((Error?) -> Void)? = nil) {
        let changesOp = CKFetchRecordZoneChangesOperation(recordZoneIDs: [ zoneId ], optionsByRecordZoneID: [ zoneId : zoneIdOptions ])
        changesOp.fetchAllChanges = true
        
        changesOp.recordZoneChangeTokensUpdatedBlock = { [weak self] zoneId, token, _ in
            guard let self = self else { return }
            self.zoneChangesToken = token
        }
        
        changesOp.recordChangedBlock = { [weak self] record in
            /// The Cloud will return the modified record since the last zoneChangesToken, we need to do local cache here.
            /// Handle the record:
            guard let self = self else { return }
            guard let syncObject = self.syncObjects.first(where: { $0.recordType == record.recordType }) else { return }
            syncObject.add(record: record)
        }
        
        changesOp.recordWithIDWasDeletedBlock = { [weak self] recordId, recordType in
            guard let self = self else { return }
            guard let syncObject = self.syncObjects.first(where: { $0.recordType == recordType }) else { return }
            syncObject.delete(recordID: recordId)
        }
        
        changesOp.recordZoneFetchCompletionBlock = { [weak self](zoneId ,token, _, _, error) in
            guard let self = self else { return }
            switch ErrorHandler.shared.resultType(with: error) {
            case .success:
                self.zoneChangesToken = token
            case .retry(let timeToWait, _):
                ErrorHandler.shared.retryOperationIfPossible(retryAfter: timeToWait, block: {
                    self.fetchChangesInZone(callback)
                })
            case .recoverableError(let reason, _):
                switch reason {
                case .changeTokenExpired:
                    /// The previousServerChangeToken value is too old and the client must re-sync from scratch
                    self.zoneChangesToken = nil
                    self.fetchChangesInZone(callback)
                default:
                    return
                }
            default:
                return
            }
        }
        
        changesOp.fetchRecordZoneChangesCompletionBlock = { error in
            
            if let error = error {
                NotificationCenter.default.post(name: Notifications.cloudKitDataPullFailed.name, object: error)
            }
            else {
                NotificationCenter.default.post(name: Notifications.cloudKitDataPullCompleted.name, object: nil)
            }
            
            callback?(error)
        }
        
        database.add(changesOp)
    }
    
    func prepare() {
        syncObjects.forEach {
            $0.pipeToEngine = { [weak self] objectsToStore, objectsToDelete in
                guard let self = self else { return }
                
                // If only downstream, we will not sync anything to CloudKit
                guard self.settings.direction != .downstream else {
                    return
                }
                
                self.syncRecordsToCloudKit(recordsToStore: objectsToStore.map { $0.record(in: self.settings.zoneId)}, recordIDsToDelete: objectsToDelete.map { $0.recordID(in: self.settings.zoneId) })
            }
        }
    }
}

extension PrivateZoneDatabaseManager {
    var databaseChangeToken: CKServerChangeToken? {
        get {
            /// For the very first time when launching, the token will be nil and the server will be giving everything on the Cloud to client
            /// In other situation just get the unarchive the data object
            guard let tokenData = UserDefaults.standard.object(forKey: IceCreamKey.databaseChangesTokenKey.value) as? Data else { return nil }
            return NSKeyedUnarchiver.unarchiveObject(with: tokenData) as? CKServerChangeToken
        }
        set {
            guard let n = newValue else {
                UserDefaults.standard.removeObject(forKey: IceCreamKey.databaseChangesTokenKey.value)
                return
            }
            let data = NSKeyedArchiver.archivedData(withRootObject: n)
            UserDefaults.standard.set(data, forKey: IceCreamKey.databaseChangesTokenKey.value)
        }
    }
    
    var zoneChangesToken: CKServerChangeToken? {
        get {
            /// For the very first time when launching, the token will be nil and the server will be giving everything on the Cloud to client
            /// In other situation just get the unarchive the data object
            guard let tokenData = UserDefaults.standard.object(forKey: settings.zoneId.zoneName + IceCreamKey.zoneChangesTokenKey.value) as? Data else { return nil }
            return NSKeyedUnarchiver.unarchiveObject(with: tokenData) as? CKServerChangeToken
        }
        set {
            guard let n = newValue else {
                UserDefaults.standard.removeObject(forKey: settings.zoneId.zoneName + IceCreamKey.zoneChangesTokenKey.value)
                return
            }
            let data = NSKeyedArchiver.archivedData(withRootObject: n)
            UserDefaults.standard.set(data, forKey: settings.zoneId.zoneName + IceCreamKey.zoneChangesTokenKey.value)
        }
    }
    
    var subscriptionIsLocallyCached: Bool {
        get {
            guard let flag = UserDefaults.standard.object(forKey: IceCreamKey.subscriptionIsLocallyCachedKey.value) as? Bool  else { return false }
            return flag
        }
        set {
            UserDefaults.standard.set(newValue, forKey: IceCreamKey.subscriptionIsLocallyCachedKey.value)
        }
    }
    
    private var zoneId: CKRecordZone.ID {
        return settings.zoneId
    }
    
    private var zoneIdOptions: CKFetchRecordZoneChangesOperation.ZoneOptions {
        
        let zoneChangesOptions = CKFetchRecordZoneChangesOperation.ZoneOptions()
        zoneChangesOptions.previousServerChangeToken = zoneChangesToken
        
        return zoneChangesOptions
    }
    
    @objc func cleanUp() {
        for syncObject in syncObjects {
            syncObject.cleanUp()
        }
    }
}

extension CKRecordZone {
    public var isCreated: Bool {
        get {
            if self.zoneID == CKRecordZone.ID.default {
                return true
            }
            
            guard let flag = UserDefaults.standard.object(forKey: zoneID.zoneName + IceCreamKey.hasCustomZoneCreatedKey.value) as? Bool else { return false }
            return flag
        }
        set {
            UserDefaults.standard.set(newValue, forKey: zoneID.zoneName + IceCreamKey.hasCustomZoneCreatedKey.value)
        }
    }
}
