#if canImport(Combine)
import Combine
#endif
import Dispatch

/// `DatabaseReader` is the protocol for all types that can fetch values from
/// an SQLite database.
///
/// It is adopted by `DatabaseQueue`, `DatabasePool`, and `DatabaseSnapshot`.
///
/// The protocol comes with isolation guarantees that describe the behavior of
/// adopting types in a multithreaded application.
///
/// Types that adopt the protocol can provide in practice stronger guarantees.
/// For example, `DatabaseQueue` provides a stronger isolation level
/// than `DatabasePool`.
///
/// **Warning**: Isolation guarantees stand as long as there is no external
/// connection to the database. Should you have to cope with external
/// connections, protect yourself with transactions, and be ready to setup a
/// [busy handler](https://www.sqlite.org/c3ref/busy_handler.html).
public protocol DatabaseReader: AnyObject {
    
    /// The database configuration
    var configuration: Configuration { get }
    
    // MARK: - Interrupting Database Operations
    
    /// This method causes any pending database operation to abort and return at
    /// its earliest opportunity.
    ///
    /// It can be called from any thread.
    ///
    /// A call to `interrupt()` that occurs when there are no running SQL
    /// statements is a no-op and has no effect on SQL statements that are
    /// started after `interrupt()` returns.
    ///
    /// A database operation that is interrupted will throw a DatabaseError with
    /// code SQLITE_INTERRUPT. If the interrupted SQL operation is an INSERT,
    /// UPDATE, or DELETE that is inside an explicit transaction, then the
    /// entire transaction will be rolled back automatically. If the rolled back
    /// transaction was started by a transaction-wrapping method such as
    /// `DatabaseWriter.write` or `Database.inTransaction`, then all database
    /// accesses will throw a DatabaseError with code SQLITE_ABORT until the
    /// wrapping method returns.
    ///
    /// For example:
    ///
    ///     try dbQueue.write { db in
    ///         // interrupted:
    ///         try Player(...).insert(db)     // throws SQLITE_INTERRUPT
    ///         // not executed:
    ///         try Player(...).insert(db)
    ///     }                                  // throws SQLITE_INTERRUPT
    ///
    ///     try dbQueue.write { db in
    ///         do {
    ///             // interrupted:
    ///             try Player(...).insert(db) // throws SQLITE_INTERRUPT
    ///         } catch { }
    ///         try Player(...).insert(db)     // throws SQLITE_ABORT
    ///     }                                  // throws SQLITE_ABORT
    ///
    ///     try dbQueue.write { db in
    ///         do {
    ///             // interrupted:
    ///             try Player(...).insert(db) // throws SQLITE_INTERRUPT
    ///         } catch { }
    ///     }                                  // throws SQLITE_ABORT
    ///
    /// When an application creates transaction without a transaction-wrapping
    /// method, no SQLITE_ABORT error warns of aborted transactions:
    ///
    ///     try dbQueue.inDatabase { db in // or dbPool.writeWithoutTransaction
    ///         try db.beginTransaction()
    ///         do {
    ///             // interrupted:
    ///             try Player(...).insert(db) // throws SQLITE_INTERRUPT
    ///         } catch { }
    ///         try Player(...).insert(db)     // success
    ///         try db.commit()                // throws SQLITE_ERROR "cannot commit - no transaction is active"
    ///     }
    ///
    /// Both SQLITE_ABORT and SQLITE_INTERRUPT errors can be checked with the
    /// `DatabaseError.isInterruptionError` property.
    func interrupt()
    
    // MARK: - Read From Database
    
    /// Synchronously executes a read-only function that accepts a database
    /// connection, and returns its result.
    ///
    /// Guarantee 1: the `value` function runs in an isolated fashion: eventual
    /// concurrent database updates are not visible from the function:
    ///
    ///     try reader.read { db in
    ///         // Those two values are guaranteed to be equal, even if the
    ///         // `player` table is modified between the two requests:
    ///         let count1 = try Player.fetchCount(db)
    ///         let count2 = try Player.fetchCount(db)
    ///     }
    ///
    ///     try reader.read { db in
    ///         // Now this value may be different:
    ///         let count = try Player.fetchCount(db)
    ///     }
    ///
    /// Guarantee 2: attempts to write in the database throw a DatabaseError
    /// whose resultCode is `SQLITE_READONLY`.
    ///
    /// It is a programmer error to call this method from another database
    /// access method:
    ///
    ///     try reader.read { db in
    ///         // Raises a fatal error
    ///         try reader.read { ... )
    ///     }
    ///
    /// - parameter value: A function that accesses the database.
    /// - throws: The error thrown by `value`, or any `DatabaseError` that would
    ///   happen while establishing the read access to the database.
    @_disfavoredOverload
    func read<T>(_ value: (Database) throws -> T) throws -> T
    
    /// Asynchronously executes a read-only function that accepts a
    /// database connection.
    ///
    /// Guarantee 1: the `value` function runs in an isolated fashion: eventual
    /// concurrent database updates are not visible from the function:
    ///
    ///     reader.asyncRead { dbResult in
    ///         do {
    ///             let db = try dbResult.get()
    ///             // Those two values are guaranteed to be equal, even if the
    ///             // `player` table is modified between the two requests:
    ///             let count1 = try Player.fetchCount(db)
    ///             let count2 = try Player.fetchCount(db)
    ///         } catch {
    ///             // handle error
    ///         }
    ///     }
    ///
    /// Guarantee 2: attempts to write in the database throw a DatabaseError
    /// whose resultCode is `SQLITE_READONLY`.
    ///
    /// - parameter value: A function that accesses the database. Its argument
    ///   is a `Result` that provides the database connection, or the failure
    ///   that would prevent establishing the read access to the database.
    func asyncRead(_ value: @escaping (Result<Database, Error>) -> Void)
    
    /// Same as asyncRead, but without retaining self
    ///
    /// :nodoc:
    func _weakAsyncRead(_ value: @escaping (Result<Database, Error>?) -> Void)
    
    /// Synchronously executes a read-only function that accepts a database
    /// connection, and returns its result.
    ///
    /// The two guarantees of the safe `read` method are lifted:
    ///
    /// The `value` function does no run in an isolated fashion: eventual
    /// concurrent database updates are visible from the function:
    ///
    ///     try reader.unsafeRead { db in
    ///         // Those two values may be different because some other thread
    ///         // may have inserted or deleted a player between the two requests:
    ///         let count1 = try Player.fetchCount(db)
    ///         let count2 = try Player.fetchCount(db)
    ///     }
    ///
    /// Cursor iterations are isolated, though:
    ///
    ///     try reader.unsafeRead { db in
    ///         // No concurrent update can mess with this iteration:
    ///         let rows = try Row.fetchCursor(db, sql: "SELECT ...")
    ///         while let row = try rows.next() { ... }
    ///     }
    ///
    /// The `value` function is not prevented from writing (DatabaseQueue, in
    /// particular, will accept database modifications).
    ///
    /// It is a programmer error to call this method from another database
    /// access method:
    ///
    ///     try reader.read { db in
    ///         // Raises a fatal error
    ///         try reader.unsafeRead { ... )
    ///     }
    ///
    /// - parameter value: A function that accesses the database.
    /// - throws: The error thrown by `value`, or any `DatabaseError` that would
    ///   happen while establishing the read access to the database.
    @_disfavoredOverload
    func unsafeRead<T>(_ value: (Database) throws -> T) throws -> T
    
    /// Asynchronously executes a read-only function that accepts a database
    /// connection, and returns its result.
    ///
    /// The two guarantees of the safe `asyncRead` method are lifted:
    ///
    /// The `value` function does no run in an isolated fashion: eventual
    /// concurrent database updates are visible from the function:
    ///
    ///     try reader.asyncUnsafeRead { dbResult in
    ///         do {
    ///             let db = try dbResult.get()
    ///             // Those two values may be different because some other thread
    ///             // may have inserted or deleted a player between the two requests:
    ///             let count1 = try Player.fetchCount(db)
    ///             let count2 = try Player.fetchCount(db)
    ///         } catch {
    ///             // handle error
    ///         }
    ///     }
    ///
    /// Cursor iterations are isolated, though:
    ///
    ///     try reader.asyncUnsafeRead { dbResult in
    ///         do {
    ///             let db = try dbResult.get()
    ///             // No concurrent update can mess with this iteration:
    ///             let rows = try Row.fetchCursor(db, sql: "SELECT ...")
    ///             while let row = try rows.next() { ... }
    ///         } catch {
    ///             // handle error
    ///         }
    ///     }
    ///
    /// The `value` function is not prevented from writing (DatabaseQueue, in
    /// particular, will accept database modifications).
    ///
    /// - parameter value: A function that accesses the database. Its argument
    ///   is a `Result` that provides the database connection, or the failure
    ///   that would prevent establishing the read access to the database.
    func asyncUnsafeRead(_ block: @escaping (Result<Database, Error>) -> Void)
    
    /// Synchronously executes a read-only function that accepts a database
    /// connection, and returns its result.
    ///
    /// The two guarantees of the safe `read` method are lifted:
    ///
    /// The `value` function does no run in an isolated fashion: eventual
    /// concurrent database updates are visible from the function:
    ///
    ///     try reader.unsafeRead { db in
    ///         // Those two values may be different because some other thread
    ///         // may have inserted or deleted a player between the two requests:
    ///         let count1 = try Player.fetchCount(db)
    ///         let count2 = try Player.fetchCount(db)
    ///     }
    ///
    /// Cursor iterations are isolated, though:
    ///
    ///     try reader.unsafeRead { db in
    ///         // No concurrent update can mess with this iteration:
    ///         let rows = try Row.fetchCursor(db, sql: "SELECT ...")
    ///         while let row = try rows.next() { ... }
    ///     }
    ///
    /// The `value` function is not prevented from writing (DatabaseQueue, in
    /// particular, will accept database modifications).
    ///
    /// This method can be called from another reading method. Yet it should be
    /// avoided because this fosters dangerous concurrency practices (lack of
    /// control of transaction boundaries).
    ///
    ///     try reader.read { db in
    ///         // OK
    ///         try reader.unsafeReentrantRead { ... )
    ///     }
    ///
    /// - parameter value: A function that accesses the database.
    /// - throws: The error thrown by `value`, or any `DatabaseError` that would
    ///   happen while establishing the read access to the database.
    func unsafeReentrantRead<T>(_ value: (Database) throws -> T) throws -> T
    
    
    // MARK: - Value Observation
    
    /// Starts a value observation.
    ///
    /// You should use the `ValueObservation.start(in:onError:onChange:)`
    /// method instead.
    ///
    /// - parameter observation: the stared observation
    /// - returns: a TransactionObserver
    ///
    /// :nodoc:
    func _add<Reducer: ValueReducer>(
        observation: ValueObservation<Reducer>,
        scheduling scheduler: ValueObservationScheduler,
        onChange: @escaping (Reducer.Value) -> Void)
    -> DatabaseCancellable
}

extension DatabaseReader {
    
    // MARK: - Backup
    
    /// Copies the database contents into another database.
    ///
    /// The `backup` method blocks the current thread until the destination
    /// database contains the same contents as the source database.
    ///
    /// When the source is a DatabasePool, concurrent writes can happen during
    /// the backup. Those writes may, or may not, be reflected in the backup,
    /// but they won't trigger any error.
    @_disfavoredOverload
    public func backup(to writer: DatabaseWriter) throws {
        try writer.writeWithoutTransaction { dbDest in
            try backup(to: dbDest)
        }
    }
    
    func backup(
        to dbDest: Database,
        afterBackupInit: (() -> Void)? = nil,
        afterBackupStep: (() -> Void)? = nil)
    throws
    {
        try read { dbFrom in
            try dbFrom.backup(
                to: dbDest,
                afterBackupInit: afterBackupInit,
                afterBackupStep: afterBackupStep)
        }
    }
}

#if canImport(Combine)
extension DatabaseReader {
    // MARK: - Publishing Database Values
    
    /// Returns a Publisher that asynchronously completes with a fetched value.
    ///
    ///     // DatabasePublishers.Read<[Player]>
    ///     let players = dbQueue.readPublisher { db in
    ///         try Player.fetchAll(db)
    ///     }
    ///
    /// Its value and completion are emitted on the main dispatch queue.
    ///
    /// - parameter value: A closure which accesses the database.
    @available(OSX 10.15, iOS 13, tvOS 13, watchOS 6, *)
    public func readPublisher<Output>(
        value: @escaping (Database) throws -> Output)
    -> DatabasePublishers.Read<Output>
    {
        readPublisher(receiveOn: DispatchQueue.main, value: value)
    }
    
    /// Returns a Publisher that asynchronously completes with a fetched value.
    ///
    ///     // DatabasePublishers.Read<[Player]>
    ///     let players = dbQueue.readPublisher(
    ///         receiveOn: DispatchQueue.global(),
    ///         value: { db in try Player.fetchAll(db) })
    ///
    /// Its value and completion are emitted on `scheduler`.
    ///
    /// - parameter scheduler: A Combine Scheduler.
    /// - parameter value: A closure which accesses the database.
    @available(OSX 10.15, iOS 13, tvOS 13, watchOS 6, *)
    public func readPublisher<S, Output>(
        receiveOn scheduler: S,
        value: @escaping (Database) throws -> Output)
    -> DatabasePublishers.Read<Output>
    where S: Scheduler
    {
        Deferred {
            Future { fulfill in
                self.asyncRead { dbResult in
                    fulfill(dbResult.flatMap { db in Result { try value(db) } })
                }
            }
        }
        .receiveValues(on: scheduler)
        .eraseToReadPublisher()
    }
}

@available(OSX 10.15, iOS 13, tvOS 13, watchOS 6, *)
extension DatabasePublishers {
    /// A publisher that reads a value from the database. It publishes exactly
    /// one element, or an error.
    ///
    /// See:
    ///
    /// - `DatabaseReader.readPublisher(receiveOn:value:)`.
    /// - `DatabaseReader.readPublisher(value:)`.
    public struct Read<Output>: Publisher {
        public typealias Output = Output
        public typealias Failure = Error
        
        fileprivate let upstream: AnyPublisher<Output, Error>
        
        public func receive<S>(subscriber: S) where S: Subscriber, Self.Failure == S.Failure, Self.Output == S.Input {
            upstream.receive(subscriber: subscriber)
        }
    }
}

@available(OSX 10.15, iOS 13, tvOS 13, watchOS 6, *)
extension Publisher where Failure == Error {
    fileprivate func eraseToReadPublisher() -> DatabasePublishers.Read<Output> {
        .init(upstream: eraseToAnyPublisher())
    }
}
#endif

#if swift(>=5.5)
@available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
extension DatabaseReader {
    
    // MARK: - Asynchronous Read From Database
    
    /// Asynchronously executes a read-only function that accepts a database
    /// connection, and returns its result.
    ///
    /// Guarantee 1: the `value` function runs in an isolated fashion: eventual
    /// concurrent database updates are not visible from the function:
    ///
    ///     try await reader.read { db in
    ///         // Those two values are guaranteed to be equal, even if the
    ///         // `player` table is modified between the two requests:
    ///         let count1 = try Player.fetchCount(db)
    ///         let count2 = try Player.fetchCount(db)
    ///     }
    ///
    ///     try await reader.read { db in
    ///         // Now this value may be different:
    ///         let count = try Player.fetchCount(db)
    ///     }
    ///
    /// Guarantee 2: attempts to write in the database throw a DatabaseError
    /// whose resultCode is `SQLITE_READONLY`.
    ///
    /// - parameter _ignored: Pass `()`. This parameter is a workaround for
    ///   https://forums.swift.org/t/async-feedback-overloads-that-differ-only-in-async/49573
    /// - parameter value: A function that accesses the database.
    /// - throws: The error thrown by `value`, or any `DatabaseError` that would
    ///   happen while establishing the read access to the database.
    public func read<T>(
        _ignored: Void = (),
        _ value: @escaping (Database) throws -> T)
    async throws -> T
    {
        try await withUnsafeThrowingContinuation { continuation in
            asyncRead { result in
                do {
                    try continuation.resume(returning: value(result.get()))
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    // MARK: - Backup
    
    /// Asynchronously copies the database contents into another database.
    ///
    /// When the source is a `DatabasePool`, concurrent writes can happen during
    /// the backup. Those writes may, or may not, be reflected in the backup,
    /// but they won't trigger any error.
    ///
    /// - parameter _ignored: Pass `()`. This parameter is a workaround for
    ///   https://forums.swift.org/t/async-feedback-overloads-that-differ-only-in-async/49573
    /// - parameter writer: The destination database
    /// - throws: Any `DatabaseError` that would happen during the backup.
    public func backup(
        _ignored: Void = (),
        to writer: DatabaseWriter)
    async throws
    {
        try await writer.writeWithoutTransaction { dbDest in
            try self.backup(to: dbDest)
        }
    }
}
#endif

extension DatabaseReader {
    // MARK: - Value Observation Support
    
    /// Adding an observation in a read-only database emits only the
    /// initial value.
    func _addReadOnly<Reducer: ValueReducer>(
        observation: ValueObservation<Reducer>,
        scheduling scheduler: ValueObservationScheduler,
        onChange: @escaping (Reducer.Value) -> Void)
    -> DatabaseCancellable
    {
        if scheduler.immediateInitialValue() {
            do {
                let value = try unsafeReentrantRead(observation.fetchValue)
                onChange(value)
            } catch {
                observation.events.didFail?(error)
            }
            return AnyDatabaseCancellable(cancel: { })
        } else {
            var isCancelled = false
            _weakAsyncRead { dbResult in
                guard !isCancelled,
                      let dbResult = dbResult
                else { return }
                
                let result = dbResult.flatMap { db in
                    Result { try observation.fetchValue(db) }
                }
                
                scheduler.schedule {
                    guard !isCancelled else { return }
                    do {
                        try onChange(result.get())
                    } catch {
                        observation.events.didFail?(error)
                    }
                }
            }
            return AnyDatabaseCancellable(cancel: { isCancelled = true })
        }
    }
}

/// A type-erased DatabaseReader
///
/// Instances of AnyDatabaseReader forward their methods to an arbitrary
/// underlying database reader.
public final class AnyDatabaseReader: DatabaseReader {
    private let base: DatabaseReader
    
    /// Creates a database reader that wraps a base database reader.
    public init(_ base: DatabaseReader) {
        self.base = base
    }
    
    public var configuration: Configuration {
        base.configuration
    }
    
    // MARK: - Interrupting Database Operations
    
    public func interrupt() {
        base.interrupt()
    }
    
    // MARK: - Reading from Database
    
    public func read<T>(_ block: (Database) throws -> T) throws -> T {
        try base.read(block)
    }
    
    public func asyncRead(_ block: @escaping (Result<Database, Error>) -> Void) {
        base.asyncRead(block)
    }
    
    /// :nodoc:
    public func _weakAsyncRead(_ block: @escaping (Result<Database, Error>?) -> Void) {
        base._weakAsyncRead(block)
    }
    
    public func unsafeRead<T>(_ block: (Database) throws -> T) throws -> T {
        try base.unsafeRead(block)
    }
    
    public func asyncUnsafeRead(_ block: @escaping (Result<Database, Error>) -> Void) {
        base.asyncUnsafeRead(block)
    }
    
    public func unsafeReentrantRead<T>(_ block: (Database) throws -> T) throws -> T {
        try base.unsafeReentrantRead(block)
    }
    
    // MARK: - Value Observation
    
    /// :nodoc:
    public func _add<Reducer: ValueReducer>(
        observation: ValueObservation<Reducer>,
        scheduling scheduler: ValueObservationScheduler,
        onChange: @escaping (Reducer.Value) -> Void)
    -> DatabaseCancellable
    {
        base._add(
            observation: observation,
            scheduling: scheduler,
            onChange: onChange)
    }
}
