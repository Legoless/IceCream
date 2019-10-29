//
//  Object+CKRecord.swift
//  IceCream
//
//  Created by 蔡越 on 11/11/2017.
//

import Foundation
import CloudKit
import RealmSwift

public protocol CKRecordConvertible {
    static var recordType: String { get }
    static var zoneID: CKRecordZone.ID { get }
    static var databaseScope: CKDatabase.Scope { get }
    
    static func ignoredCloudProperties () -> [String]
    
    var recordID: CKRecord.ID { get }
    var record: CKRecord { get }

    var isDeleted: Bool { get }
}

extension CKRecordConvertible where Self: Object {
    
    public static var databaseScope: CKDatabase.Scope {
        return .private
    }
    
    public static var recordType: String {
        return className()
    }
    
    public static func ignoredCloudProperties() -> [String] {
        return []
    }
    
    public static var zoneID: CKRecordZone.ID {
        switch Self.databaseScope {
        case .private:
            return CKRecordZone.ID(zoneName: "\(recordType)sZone", ownerName: CKCurrentUserDefaultName)
        case .public:
            return CKRecordZone.default().zoneID
        default:
            fatalError("Shared Database is not supported now")
        }
    }
    
    /// recordName : this is the unique identifier for the record, used to locate records on the database. We can create our own ID or leave it to CloudKit to generate a random UUID.
    /// For more: https://medium.com/@guilhermerambo/synchronizing-data-with-cloudkit-94c6246a3fda
    public var recordID: CKRecord.ID {
        guard let sharedSchema = Self.sharedSchema() else {
            fatalError("No schema settled. Go to Realm Community to seek more help.")
        }
        
        guard let primaryKeyProperty = sharedSchema.primaryKeyProperty else {
            fatalError("You should set a primary key on your Realm object")
        }
        
        switch primaryKeyProperty.type {
        case .string:
            if let primaryValueString = self[primaryKeyProperty.name] as? String {
                return CKRecord.ID(recordName: primaryValueString, zoneID: Self.zoneID)
            } else {
                assertionFailure("\(primaryKeyProperty.name)'s value should be String type")
            }
        case .int:
            if let primaryValueInt = self[primaryKeyProperty.name] as? Int {
                return CKRecord.ID(recordName: "\(primaryValueInt)", zoneID: Self.zoneID)
            } else {
                assertionFailure("\(primaryKeyProperty.name)'s value should be Int type")
            }
        default:
            assertionFailure("Primary key should be String or Int")
        }
        fatalError("Should have a reasonable recordID")
    }
    
    // Simultaneously init CKRecord with zoneID and recordID, thanks to this guy: https://stackoverflow.com/questions/45429133/how-to-initialize-ckrecord-with-both-zoneid-and-recordid
    public var record: CKRecord {
        let r = CKRecord(recordType: Self.recordType, recordID: recordID)
        let properties = objectSchema.properties
        let ignoredProperties = Self.ignoredCloudProperties()
        
        for prop in properties {
            
            if ignoredProperties.contains(prop.name) {
                continue
            }
            
            let item = self[prop.name]
            
            if prop.isArray {
                switch prop.type {
                case .int:
                    guard let list = item as? List<Int>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .string:
                    guard let list = item as? List<String>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .bool:
                    guard let list = item as? List<Bool>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .float:
                    guard let list = item as? List<Float>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .double:
                    guard let list = item as? List<Double>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .data:
                    guard let list = item as? List<Data>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                case .date:
                    guard let list = item as? List<Date>, !list.isEmpty else { break }
                    let array = Array(list)
                    r[prop.name] = array as CKRecordValue
                default:
                    break
                    /// Other inner types of List is not supported yet
                }
                continue
            }
            
            switch prop.type {
            case .int, .string, .bool, .date, .float, .double, .data:
                r[prop.name] = item as? CKRecordValue
            case .object:
                guard let objectName = prop.objectClassName else { break }
                // If object is CreamAsset, set record with its wrapped CKAsset value
                if objectName == CreamAsset.className(), let creamAsset = item as? CreamAsset {
                    r[prop.name] = creamAsset.asset
                } else if let owner = item as? CKRecordConvertible {
                    // Handle to-one relationship: https://realm.io/docs/swift/latest/#many-to-one
                    // So the owner Object has to conform to CKRecordConvertible protocol
                    r[prop.name] = CKRecord.Reference(recordID: owner.recordID, action: .none)
                } else {
                    /// Just a warm hint:
                    /// When we set nil to the property of a CKRecord, that record's property will be hidden in the CloudKit Dashboard
                    r[prop.name] = nil
                }
                // To-many relationship is not supported yet.
            default:
                break
            }
            
        }
        return r
    }
    
}

extension CKRecordConvertible where Self: Object {
    static func parseFromRecord(record: CKRecord, realm: Realm) -> Self? {
        let o = Self()
        let ignoredProperties = Self.ignoredCloudProperties()
        for prop in o.objectSchema.properties {
            if ignoredProperties.contains(prop.name) {
                continue
            }
            
            var recordValue: Any?
            
            if prop.isArray {
                switch prop.type {
                case .int:
                    guard let value = record.value(forKey: prop.name) as? [Int] else { break }
                    let list = List<Int>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .string:
                    guard let value = record.value(forKey: prop.name) as? [String] else { break }
                    let list = List<String>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .bool:
                    guard let value = record.value(forKey: prop.name) as? [Bool] else { break }
                    let list = List<Bool>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .float:
                    guard let value = record.value(forKey: prop.name) as? [Float] else { break }
                    let list = List<Float>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .double:
                    guard let value = record.value(forKey: prop.name) as? [Double] else { break }
                    let list = List<Double>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .data:
                    guard let value = record.value(forKey: prop.name) as? [Data] else { break }
                    let list = List<Data>()
                    list.append(objectsIn: value)
                    recordValue = list
                case .date:
                    guard let value = record.value(forKey: prop.name) as? [Date] else { break }
                    let list = List<Date>()
                    list.append(objectsIn: value)
                    recordValue = list
                default:
                    break
                }
                o.setValue(recordValue, forKey: prop.name)
                continue
            }
            
            switch prop.type {
            case .int:
                recordValue = record.value(forKey: prop.name) as? Int
            case .string:
                recordValue = record.value(forKey: prop.name) as? String
            case .bool:
                recordValue = record.value(forKey: prop.name) as? Bool
            case .date:
                recordValue = record.value(forKey: prop.name) as? Date
            case .float:
                recordValue = record.value(forKey: prop.name) as? Float
            case .double:
                recordValue = record.value(forKey: prop.name) as? Double
            case .data:
                recordValue = record.value(forKey: prop.name) as? Data
            case .object:
                if let asset = record.value(forKey: prop.name) as? CKAsset {
                    recordValue = CreamAsset.parse(from: prop.name, record: record, asset: asset)
                } else if let owner = record.value(forKey: prop.name) as? CKRecord.Reference,
                    let ownerType = prop.objectClassName,
                    let schema = realm.schema.objectSchema.first(where: { $0.className == ownerType })
                {
                    primaryKeyForRecordID(recordID: owner.recordID, schema: schema).flatMap {
                        recordValue = realm.dynamicObject(ofType: ownerType, forPrimaryKey: $0)
                    }
                    // Because we use the primaryKey as recordName when object converting to CKRecord
                }
            default:
                print("Other types will be supported in the future.")
            }
            if recordValue != nil || (recordValue == nil && prop.isOptional) {
                o.setValue(recordValue, forKey: prop.name)
            }
        }
        return o
    }
    
    /// The primaryKey in Realm could be type of Int or String. However the `recordName` is a String type, we need to make a check.
    /// The reversed process happens in `recordID` property in `CKRecordConvertible` protocol.
    ///
    /// - Parameter recordID: the recordID that CloudKit sent to us
    /// - Returns: the specific value of primaryKey in Realm
    static func primaryKeyForRecordID(recordID: CKRecord.ID, schema: ObjectSchema? = nil) -> Any? {
        let schema = schema ?? Self().objectSchema
        guard let objectPrimaryKeyType = schema.primaryKeyProperty?.type else { return nil }
        switch objectPrimaryKeyType {
        case .string:
            return recordID.recordName
        case .int:
            return Int(recordID.recordName)
        default:
            fatalError("The type of object primaryKey should be String or Int")
        }
    }
}
