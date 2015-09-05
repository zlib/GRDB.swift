// MARK: - RowModel

/**
RowModel is a class that wraps a table row, or the result of any query. It is
designed to be subclassed.

Subclasses opt in RowModel features by overriding all or part of the core
methods that define their relationship with the database:

- setDatabaseValue(_:forColumn:)
- databaseTable
- storedDatabaseDictionary
*/
public class RowModel {
    
    /// The result of the RowModel.delete() method
    public enum DeletionResult {
        /// A row was deleted.
        case RowDeleted
        
        /// No row was deleted.
        case NoRowDeleted
    }
    
    
    // MARK: - Core methods
    
    /**
    Returns a table definition.
    
    The insert, update, save, delete and reload methods require it: they raise
    a fatal error if databaseTableName is nil.
    
    The implementation of the base class RowModel returns nil.
    */
    public class var databaseTableName: String? {
        return nil
    }
    
    /**
    Returns the values that should be stored in the database.
    
    Subclasses must include primary key columns, if any, in the returned
    dictionary.
    
    The implementation of the base class RowModel returns an empty dictionary.
    */
    public var storedDatabaseDictionary: [String: DatabaseValueConvertible?] {
        return [:]
    }
    
    /**
    Updates `self` with a database value.
    
    The implementation of the base class RowModel does nothing.
    
    - parameter dbv: A DatabaseValue.
    - parameter column: A column name.
    */
    public func setDatabaseValue(dbv: DatabaseValue, forColumn column: String) {
    }
    
    
    // MARK: - Initializers
    
    /**
    Initializes a RowModel.
    
    The returned rowModel is *edited*.
    */
    public init() {
        // IMPLEMENTATION NOTE
        //
        // This initializer is defined so that a subclass can be defined
        // without any custom initializer.
    }
    
    /**
    Initializes a RowModel from a row.
    
    The returned rowModel is *edited*.
    
    - parameter row: A Row
    */
    required public init(row: Row) {
        // IMPLEMENTATION NOTE
        //
        // Swift requires a required initializer so that we can fetch RowModels
        // in SelectStatement.fetch<RowModel: GRDB.RowModel>(type: RowModel.Type, arguments: StatementArguments? = nil) -> AnySequence<RowModel>
        //
        // This required initializer *can not* be the simple init(), because it
        // would prevent subclasses to provide handy initializers made of
        // optional arguments like init(firstName: String? = nil, lastName: String? = nil).
        // See rdar://22554816 for more information.
        //
        // OK so the only initializer that we can require in init(row:Row).
        //
        // IMPLEMENTATION NOTE
        //
        // This initializer returns an edited model because the row may not
        // come from the database.
        
        updateFromRow(row)
    }
    
    
    // MARK: - Events
    
    /**
    Called after a RowModel has been fetched or reloaded.
    
    *Important*: subclasses must invoke super's implementation.
    */
    public func didFetch() {
    }
    
    
    // MARK: - Update
    
    /**
    Updates self from a row.
    
    *Important*: subclasses must invoke super's implementation.
    */
    public func updateFromRow(row: Row) {
        for (column, databaseValue) in row {
            setDatabaseValue(databaseValue, forColumn: column)
        }
    }
    
    /**
    Updates `self` from `other.storedDatabaseDictionary`.
    */
    public func copyDatabaseValuesFrom(other: RowModel) {
        updateFromRow(Row(dictionary: other.storedDatabaseDictionary))
    }
    
    
    // MARK: - Changes
    
    /**
    A boolean that indicates whether the row model has changes that have not
    been saved.
    
    This flag is purely informative, and does not prevent insert(), update(),
    save() and reload() to perform their database queries. Yet you can prevent
    queries that are known to be pointless, as in the following example:
        
        let json = ...
    
        // Fetches or create a new person given its ID:
        let person = Person.fetchOne(db, primaryKey: json["id"]) ?? Person()
    
        // Apply json payload:
        person.updateFromJSON(json)
                 
        // Saves the person if it is edited (fetched then modified, or created):
        if person.databaseEdited {
            person.save(db) // inserts or updates
        }
    
    Precisely speaking, a row model is edited if its *storedDatabaseDictionary*
    has been changed since last database synchronization (fetch, update,
    insert). Comparison is performed on *values*: setting a property to the same
    value does not trigger the edited flag.
    
    You can rely on the RowModel base class to compute this flag for you, or you
    may set it to true or false when you know better. Setting it to false does
    not prevent it from turning true on subsequent modifications of the row model.
    */
    public var databaseEdited: Bool {
        get {
            guard let referenceRow = referenceRow else {
                // No reference row => edited
                return true
            }
            
            // All stored database values must match reference database values
            for (column, storedValue) in storedDatabaseDictionary {
                guard let referenceDatabaseValue = referenceRow[column] else {
                    return true
                }
                let storedDatabaseValue = storedValue?.databaseValue ?? .Null
                if storedDatabaseValue != referenceDatabaseValue {
                    return true
                }
            }
            return false
        }
        set {
            if newValue {
                referenceRow = nil
            } else {
                referenceRow = Row(dictionary: storedDatabaseDictionary)
            }
        }
    }
    
    /// Reference row for the *databaseEdited* property.
    private var referenceRow: Row?
    

    // MARK: - CRUD
    
    /**
    Executes an INSERT statement to insert the row model.
    
    On success, this method sets the *databaseEdited* flag to false.
    
    This method is guaranteed to have inserted a row in the database if it
    returns without error.
    
    - parameter db: A Database.
    - throws: A DatabaseError whenever a SQLite error occurs.
    */
    public func insert(db: Database) throws {
        let dataMapper = DataMapper(db, rowModel: self)
        let changes = try dataMapper.insertStatement(db).execute()
        
        // Update RowID column if needed
        if case .Managed(let managedColumn) = dataMapper.primaryKey {
            guard let rowID = dataMapper.storedDatabaseDictionary[managedColumn] else {
                fatalError("\(self.dynamicType).storedDatabaseDictionary must return the value for the primary key `(managedColumn)`")
            }
            if rowID == nil {
                // IMPLEMENTATION NOTE:
                //
                // We update the ID with updateFromRow(), and not
                // setDatabaseValue(_:forColumn:). Rationale:
                //
                // 1. If subclass updates its ID in setDatabaseValue(), then the
                //    default updateFromRow() runs, which calls
                //    setDatabaseValue(), and updates the ID.
                //
                // 2. If subclass overrides updateFromRow() and updates its ID
                //    in setDatabaseValue(), then the subclasses calls super
                //    from updateFromRow() (as it is required to do), which in
                //    turns call setDatabaseValue(), and updates the ID.
                //
                // 3. If subclass overrides updateFromRow() and updates its ID
                //    in updateFromRow(), not in setDatabaseValue(), which it is
                //    allowed to do, then using setDatabaseValue() would not
                //    update the ID.
                updateFromRow(Row(dictionary: [managedColumn: changes.insertedRowID]))
            }
        }
        
        databaseEdited = false
    }
    
    /**
    Executes an UPDATE statement to update the row model.
    
    On success, this method sets the *databaseEdited* flag to false.
    
    This method is guaranteed to have updated a row in the database if it
    returns without error.
    
    - parameter db: A Database.
    - throws: A DatabaseError is thrown whenever a SQLite error occurs.
              RowModelError.RowModelNotFound is thrown if the primary key does
              not match any row in the database and row model could not be
              updated.
    */
    public func update(db: Database) throws {
        // We'll throw RowModelError.RowModelNotFound if rowModel does not exist.
        let exists: Bool
        
        if let statement = try DataMapper(db, rowModel: self).updateStatement(db) {
            let changes = try statement.execute()
            exists = changes.changedRowCount > 0
        } else {
            // No statement means that there is no column to update.
            //
            // I remember opening rdar://problem/10236982 because CoreData
            // was crashing with entities without any attribute. So let's
            // accept RowModel that don't have any column to update.
            exists = self.exists(db)
        }
        
        if !exists {
            throw RowModelError.RowModelNotFound(self)
        }
        
        databaseEdited = false
    }
    
    /**
    Saves the row model in the database.
    
    If the row model has a non-nil primary key and a matching row in the
    database, this method performs an update.
    
    Otherwise, performs an insert.
    
    On success, this method sets the *databaseEdited* flag to false.
    
    This method is guaranteed to have inserted or updated a row in the database
    if it returns without error.
    
    - parameter db: A Database.
    - throws: A DatabaseError whenever a SQLite error occurs, or errors thrown
              by update().
    */
    final public func save(db: Database) throws {
        // Make sure we call self.insert and self.update so that classes that
        // override insert or save have opportunity to perform their custom job.
        
        if DataMapper(db, rowModel: self).resolvingPrimaryKeyDictionary == nil {
            return try insert(db)
        }
        
        do {
            try update(db)
        } catch RowModelError.RowModelNotFound {
            return try insert(db)
        }
    }
    
    /**
    Executes a DELETE statement to delete the row model.
    
    On success, this method sets the *databaseEdited* flag to true.
    
    - parameter db: A Database.
    - returns: Whether a row was deleted or not.
    - throws: A DatabaseError is thrown whenever a SQLite error occurs.
    */
    public func delete(db: Database) throws -> DeletionResult {
        let changes = try DataMapper(db, rowModel: self).deleteStatement(db).execute()
        
        // Future calls to update will throw RowModelNotFound. Make the user
        // a favor and make sure this error is thrown even if she checks the
        // databaseEdited flag:
        databaseEdited = true
        
        if changes.changedRowCount > 0 {
            return .RowDeleted
        } else {
            return .NoRowDeleted
        }
    }
    
    /**
    Executes a SELECT statetement to reload the row model.
    
    On success, this method sets the *databaseEdited* flag to false.
    
    - parameter db: A Database.
    - throws: RowModelError.RowModelNotFound is thrown if the primary key does
              not match any row in the database and row model could not be
              reloaded.
    */
    final public func reload(db: Database) throws {
        let statement = DataMapper(db, rowModel: self).reloadStatement(db)
        if let row = Row.fetchOne(statement) {
            updateFromRow(row)
            referenceRow = row
            didFetch()
        } else {
            throw RowModelError.RowModelNotFound(self)
        }
    }
    
    /**
    Returns true if and only if the primary key matches a row in the database.
    
    - parameter db: A Database.
    - returns: Whether the primary key matches a row in the database.
    */
    final public func exists(db: Database) -> Bool {
        return (Row.fetchOne(DataMapper(db, rowModel: self).existsStatement(db)) != nil)
    }
    
    
    // MARK: - DataMapper
    
    /// DataMapper takes care of RowModel CRUD
    private final class DataMapper {
        
        /// The rowModel type
        let rowModel: ImmutableRowModelType
        
        /// DataMapper keeps a copy the rowModel's storedDatabaseDictionary, so
        /// that this dictionary is built once whatever the database operation.
        /// It is guaranteed to have at least one (key, value) pair.
        let storedDatabaseDictionary: [String: DatabaseValueConvertible?]
        
        /// The table name
        let databaseTableName: String
        
        /// The table primary key
        let primaryKey: PrimaryKey
        
        
        // MARK: - Primary Key
        
        /**
        An excerpt from storedDatabaseDictionary whose keys are primary key
        columns.
        
        It is nil when rowModel has no primary key.
        */
        lazy var primaryKeyDictionary: [String: DatabaseValueConvertible?]? = { [unowned self] in
            let columns = self.primaryKey.columns
            guard columns.count > 0 else {
                return nil
            }
            let storedDatabaseDictionary = self.storedDatabaseDictionary
            var dictionary: [String: DatabaseValueConvertible?] = [:]
            for column in columns {
                dictionary[column] = storedDatabaseDictionary[column]
            }
            return dictionary
            }()
        
        /**
        An excerpt from storedDatabaseDictionary whose keys are primary key
        columns. It is able to resolve a row in the database.
        
        It is nil when the primaryKeyDictionary is nil or unable to identify a
        row in the database.
        */
        lazy var resolvingPrimaryKeyDictionary: [String: DatabaseValueConvertible?]? = { [unowned self] in
            // IMPLEMENTATION NOTE
            //
            // https://www.sqlite.org/lang_createtable.html
            //
            // > According to the SQL standard, PRIMARY KEY should always
            // > imply NOT NULL. Unfortunately, due to a bug in some early
            // > versions, this is not the case in SQLite. Unless the column
            // > is an INTEGER PRIMARY KEY or the table is a WITHOUT ROWID
            // > table or the column is declared NOT NULL, SQLite allows
            // > NULL values in a PRIMARY KEY column. SQLite could be fixed
            // > to conform to the standard, but doing so might break legacy
            // > applications. Hence, it has been decided to merely document
            // > the fact that SQLite allowing NULLs in most PRIMARY KEY
            // > columns.
            //
            // What we implement: we consider that the primary key is missing if
            // and only if *all* columns of the primary key are NULL.
            //
            // For tables with a single column primary key, we comply to the
            // SQL standard.
            //
            // For tables with multi-column primary keys, we let the user
            // store NULL in all but one columns of the primary key.
            
            guard let dictionary = self.primaryKeyDictionary else {
                return nil
            }
            for case let value? in dictionary.values {
                return dictionary
            }
            return nil
            }()
        
        
        // MARK: - Initializer
        
        init(_ db: Database, rowModel: ImmutableRowModelType) {
            // Fail early if databaseTable is nil (not overriden)
            guard let databaseTableName = rowModel.dynamicType.databaseTableName else {
                fatalError("Nil returned from \(rowModel.dynamicType).databaseTableName")
            }

            // Fail early if database table does not exist.
            guard let primaryKey = db.primaryKeyForTable(named: databaseTableName) else {
                fatalError("Table \(databaseTableName) does not exist. See \(rowModel.dynamicType).databaseTableName")
            }

            // Fail early if storedDatabaseDictionary is empty (not overriden)
            let storedDatabaseDictionary = rowModel.storedDatabaseDictionary
            guard storedDatabaseDictionary.count > 0 else {
                fatalError("Invalid empty dictionary returned from \(rowModel.dynamicType).storedDatabaseDictionary")
            }
            
            self.rowModel = rowModel
            self.storedDatabaseDictionary = storedDatabaseDictionary
            self.databaseTableName = databaseTableName
            self.primaryKey = primaryKey
        }
        
        
        // MARK: - Statement builders
        
        func insertStatement(db: Database) throws -> UpdateStatement {
            let insertStatement = try db.updateStatement(DataMapper.insertSQL(tableName: databaseTableName, insertedColumns: Array(storedDatabaseDictionary.keys)))
            insertStatement.arguments = StatementArguments(storedDatabaseDictionary.values)
            return insertStatement
        }
        
        /// Returns nil if there is no column to update
        func updateStatement(db: Database) throws -> UpdateStatement? {
            // Fail early if primary key does not resolve to a database row.
            guard let primaryKeyDictionary = resolvingPrimaryKeyDictionary else {
                fatalError("Invalid primary key in \(rowModel)")
            }
            
            // Don't update primary key columns
            var updatedDictionary = storedDatabaseDictionary
            for column in primaryKeyDictionary.keys {
                updatedDictionary.removeValueForKey(column)
            }
            
            // We need something to update.
            guard updatedDictionary.count > 0 else {
                return nil
            }
            
            // Update
            let updateStatement = try db.updateStatement(DataMapper.updateSQL(tableName: databaseTableName, updatedColumns: Array(updatedDictionary.keys), conditionColumns: Array(primaryKeyDictionary.keys)))
            updateStatement.arguments = StatementArguments(Array(updatedDictionary.values) + Array(primaryKeyDictionary.values))
            return updateStatement
        }
        
        func deleteStatement(db: Database) throws -> UpdateStatement {
            // Fail early if primary key does not resolve to a database row.
            guard let primaryKeyDictionary = resolvingPrimaryKeyDictionary else {
                fatalError("Invalid primary key in \(rowModel)")
            }
            
            // Delete
            let deleteStatement = try db.updateStatement(DataMapper.deleteSQL(tableName: databaseTableName, conditionColumns: Array(primaryKeyDictionary.keys)))
            deleteStatement.arguments = StatementArguments(primaryKeyDictionary.values)
            return deleteStatement
        }
        
        func reloadStatement(db: Database) -> SelectStatement {
            // Fail early if primary key does not resolve to a database row.
            guard let primaryKeyDictionary = resolvingPrimaryKeyDictionary else {
                fatalError("Invalid primary key in \(rowModel)")
            }
            
            // Fetch
            let reloadStatement = db.selectStatement(DataMapper.reloadSQL(tableName: databaseTableName, conditionColumns: Array(primaryKeyDictionary.keys)))
            reloadStatement.arguments = StatementArguments(primaryKeyDictionary.values)
            return reloadStatement
        }
        
        /// SELECT statement that returns a row if and only if the primary key
        /// matches a row in the database.
        func existsStatement(db: Database) -> SelectStatement {
            // Fail early if primary key does not resolve to a database row.
            guard let primaryKeyDictionary = resolvingPrimaryKeyDictionary else {
                fatalError("Invalid primary key in \(rowModel)")
            }
            
            // Fetch
            let existsStatement = db.selectStatement(DataMapper.existsSQL(tableName: databaseTableName, conditionColumns: Array(primaryKeyDictionary.keys)))
            existsStatement.arguments = StatementArguments(primaryKeyDictionary.values)
            return existsStatement
        }
        
        
        // MARK: - SQL query builders
        
        class func insertSQL(tableName tableName: String, insertedColumns: [String]) -> String {
            let columnSQL = insertedColumns.map { $0.quotedDatabaseIdentifier }.joinWithSeparator(",")
            let valuesSQL = [String](count: insertedColumns.count, repeatedValue: "?").joinWithSeparator(",")
            return "INSERT INTO \(tableName.quotedDatabaseIdentifier) (\(columnSQL)) VALUES (\(valuesSQL))"
        }
        
        class func updateSQL(tableName tableName: String, updatedColumns: [String], conditionColumns: [String]) -> String {
            let updateSQL = updatedColumns.map { "\($0.quotedDatabaseIdentifier)=?" }.joinWithSeparator(",")
            return "UPDATE \(tableName.quotedDatabaseIdentifier) SET \(updateSQL) WHERE \(whereSQL(conditionColumns))"
        }
        
        class func deleteSQL(tableName tableName: String, conditionColumns: [String]) -> String {
            return "DELETE FROM \(tableName.quotedDatabaseIdentifier) WHERE \(whereSQL(conditionColumns))"
        }
        
        class func existsSQL(tableName tableName: String, conditionColumns: [String]) -> String {
            return "SELECT 1 FROM \(tableName.quotedDatabaseIdentifier) WHERE \(whereSQL(conditionColumns))"
        }

        class func reloadSQL(tableName tableName: String, conditionColumns: [String]) -> String {
            return "SELECT * FROM \(tableName.quotedDatabaseIdentifier) WHERE \(whereSQL(conditionColumns))"
        }
        
        class func whereSQL(conditionColumns: [String]) -> String {
            return conditionColumns.map { "\($0.quotedDatabaseIdentifier)=?" }.joinWithSeparator(" AND ")
        }
    }
}


// MARK: - ImmutableRowModelType

/// An immutable view to RowModel
protocol ImmutableRowModelType {
    static var databaseTableName: String? { get }
    var storedDatabaseDictionary: [String: DatabaseValueConvertible?] { get }
}

extension RowModel : ImmutableRowModelType { }


// MARK: - CustomStringConvertible

extension RowModel : CustomStringConvertible {
    /// A textual representation of `self`.
    public var description: String {
        return "<\(self.dynamicType)"
            + storedDatabaseDictionary.map { (key, value) in
                if let value = value {
                    return " \(key):\(String(reflecting: value))"
                } else {
                    return " \(key):nil"
                }
                }.joinWithSeparator("")
            + ">"
    }
}


// MARK: - RowModelError

/// A RowModel-specific error
public enum RowModelError: ErrorType {
    
    /// No matching row could be found in the database.
    case RowModelNotFound(RowModel)
}

extension RowModelError : CustomStringConvertible {
    /// A textual representation of `self`.
    public var description: String {
        switch self {
        case .RowModelNotFound(let rowModel):
            return "RowModel not found: \(rowModel)"
        }
    }
}


// MARK: - Fetching

/// FetchableRowModel is a protocol adopted by RowModel, which allows fetching
/// RowModel instances from the database.
public protocol FetchableRowModel { }

/// RowModel adopts FetchableRowModel, a protocol which allows fetching RowModel
/// instances from the database.
extension RowModel: FetchableRowModel { }

/// FetchableRowModel is a protocol adopted by RowModel, which allows fetching
/// RowModel instances from the database.
public extension FetchableRowModel where Self : RowModel {
    
    // MARK: - Fetching From SelectStatement
    
    /**
    Fetches a lazy sequence of RowModels.
        
        let statement = db.selectStatement("SELECT * FROM persons")
        let persons = Person.fetch(statement) // AnySequence<Person>

    - parameter statement: The statement to run.
    - parameter arguments: Optional statement arguments.
    - returns: A lazy sequence of row models.
    */
    public static func fetch(statement: SelectStatement, arguments: StatementArguments? = nil) -> AnySequence<Self> {
        let rowSequence = Row.fetch(statement, arguments: arguments)
        func generate() -> AnyGenerator<Self> {
            let rowGenerator = rowSequence.generate()
            return anyGenerator {
                guard let row = rowGenerator.next() else {
                    return nil
                }
                
                let rowModel = Self.init(row: row)
                rowModel.referenceRow = row // Takes care of the databaseEdited flag. If the row does not contain all columns, the model remains edited.
                rowModel.didFetch()
                return rowModel
            }
        }
        return AnySequence(generate)
    }
    
    /**
    Fetches an array of RowModels.
        
        let statement = db.selectStatement("SELECT * FROM persons")
        let persons = Person.fetchAll(statement) // [Person]
    
    - parameter statement: The statement to run.
    - parameter arguments: Optional statement arguments.
    - returns: An array of row models.
    */
    public static func fetchAll(statement: SelectStatement, arguments: StatementArguments? = nil) -> [Self] {
        return Array(fetch(statement, arguments: arguments))
    }
    
    /**
    Fetches a single RowModel.
        
        let statement = db.selectStatement("SELECT * FROM persons")
        let persons = Person.fetchOne(statement) // Person?
    
    - parameter statement: The statement to run.
    - parameter arguments: Optional statement arguments.
    - returns: An optional row model.
    */
    public static func fetchOne(statement: SelectStatement, arguments: StatementArguments? = nil) -> Self? {
        guard let first = fetch(statement, arguments: arguments).generate().next() else {
            return nil
        }
        return first
    }
    
    
    // MARK: - Fetching From Database
    
    /**
    Fetches a lazy sequence of RowModels.

        let persons = Person.fetch(db, "SELECT * FROM persons") // AnySequence<Person>

    - parameter db: A Database.
    - parameter sql: An SQL query.
    - parameter arguments: Optional statement arguments.
    - returns: A lazy sequence of row models.
    */
    public static func fetch(db: Database, _ sql: String, arguments: StatementArguments? = nil) -> AnySequence<Self> {
        return fetch(db.selectStatement(sql), arguments: arguments)
    }

    /**
    Fetches an array sequence of RowModels.

        let persons = Person.fetchAll(db, "SELECT * FROM persons") // [Person]

    - parameter db: A Database.
    - parameter sql: An SQL query.
    - parameter arguments: Optional statement arguments.
    - returns: An array of row models.
    */
    public static func fetchAll(db: Database, _ sql: String, arguments: StatementArguments? = nil) -> [Self] {
        return Array(fetch(db, sql, arguments: arguments))
    }

    /**
    Fetches a single RowModel.

        let person = Person.fetchOne(db, "SELECT * FROM persons") // Person?

    - parameter db: A Database.
    - parameter sql: An SQL query.
    - parameter arguments: Optional statement arguments.
    - returns: An optional row model.
    */
    public static func fetchOne(db: Database, _ sql: String, arguments: StatementArguments? = nil) -> Self? {
        if let first = fetch(db, sql, arguments: arguments).generate().next() {
            // one row containing an optional value
            return first
        } else {
            // no row
            return nil
        }
    }

    /**
    Fetches a single RowModel by primary key.

        let person = Person.fetchOne(db, primaryKey: 123) // Person?

    - parameter db: A Database.
    - parameter primaryKey: A value.
    - returns: An optional row model.
    */
    public static func fetchOne(db: Database, primaryKey primaryKeyValue: DatabaseValueConvertible?) -> Self? {
        // Fail early if databaseTable is nil (not overriden)
        guard let databaseTableName = self.databaseTableName else {
            fatalError("Nil returned from \(self).databaseTableName")
        }
        
        // Fail early if database table does not exist.
        guard let primaryKey = db.primaryKeyForTable(named: databaseTableName) else {
            fatalError("Table \(databaseTableName) does not exist. See \(self).databaseTableName")
        }
        
        // Fail early if database table has not one column in its primary key
        let columns = primaryKey.columns
        guard columns.count == 1 else {
            if columns.count == 0 {
                fatalError("Table \(databaseTableName) has no primary key. See \(self).databaseTableName")
            } else {
                fatalError("Table \(databaseTableName) has a multi-column primary key. See \(self).databaseTableName")
            }
        }
        
        guard let primaryKeyValue = primaryKeyValue else {
            return nil
        }
        
        let sql = "SELECT * FROM \(databaseTableName.quotedDatabaseIdentifier) WHERE \(columns.first!.quotedDatabaseIdentifier) = ?"
        return fetchOne(db.selectStatement(sql), arguments: [primaryKeyValue])
    }
    
    /**
    Fetches a single RowModel given a key.

        let person = Person.fetchOne(db, key: ["name": Arthur"]) // Person?

    - parameter db: A Database.
    - parameter key: A dictionary of values.
    - returns: An optional row model.
    */
    public static func fetchOne(db: Database, key dictionary: [String: DatabaseValueConvertible?]) -> Self? {
        // Fail early if databaseTable is nil (not overriden)
        guard let databaseTableName = self.databaseTableName else {
            fatalError("Nil returned from \(self).databaseTableName")
        }
        
        // Fail early if key is empty.
        guard dictionary.count > 0 else {
            fatalError("Invalid empty key")
        }
        
        let whereSQL = dictionary.keys.map { column in "\(column.quotedDatabaseIdentifier)=?" }.joinWithSeparator(" AND ")
        let sql = "SELECT * FROM \(databaseTableName.quotedDatabaseIdentifier) WHERE \(whereSQL)"
        return fetchOne(db.selectStatement(sql), arguments: StatementArguments(dictionary.values))
    }
}
