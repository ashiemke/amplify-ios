//
// Copyright Amazon.com Inc. or its affiliates.
// All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import XCTest
import Combine

@testable import Amplify
@testable import AmplifyTestCommon
@testable import AWSPluginsCore
@testable import AWSDataStoreCategoryPlugin

class DataStoreObserveQueryOperationTests: XCTestCase {

    var storageEngine: MockStorageEngineBehavior!
    var dataStorePublisher: ModelSubcriptionBehavior!

    override func setUp() {
        ModelRegistry.register(modelType: Post.self)
        storageEngine = MockStorageEngineBehavior()
        dataStorePublisher = DataStorePublisher()
    }

    /// After the query finishes, observed item changes will generate a snapshot.
    ///
    /// - Given:  The operation has started and the initial query has completed.
    /// - When:
    ///    -  Item change occurs.
    /// - Then:
    ///    - Receive a snapshot with the item changed
    ///
    func testItemChangedWillGenerateSnapshot() throws {
        let firstSnapshot = expectation(description: "first query snapshots")
        let secondSnapshot = expectation(description: "second query snapshots")
        var querySnapshots = [DataStoreQuerySnapshot<Post>]()
        let operation = AWSDataStoreObseverQueryOperation(
            modelType: Post.self,
            modelSchema: Post.schema,
            predicate: nil,
            sortInput: nil,
            storageEngine: storageEngine,
            dataStorePublisher: dataStorePublisher,
            isDataStoreReady: false)

        let sink = operation.publisher.sink { completed in
            switch completed {
            case .finished:
                break
            case .failure(let error):
                XCTFail("Failed with error \(error)")
            }
        } receiveValue: { querySnapshot in
            querySnapshots.append(querySnapshot)
            if querySnapshots.count == 1 {
                firstSnapshot.fulfill()
            } else if querySnapshots.count == 2 {
                secondSnapshot.fulfill()
            }
        }
        let queue = OperationQueue()
        queue.addOperation(operation)
        wait(for: [firstSnapshot], timeout: 1)

        let post = try createPost(id: "1")
        dataStorePublisher.send(input: post)
        wait(for: [secondSnapshot], timeout: 10)

        XCTAssertEqual(querySnapshots.count, 2)
        XCTAssertEqual(querySnapshots[0].itemsChanged.count, 0)
        XCTAssertEqual(querySnapshots[1].itemsChanged.count, 1)
        sink.cancel()
    }

    /// Multiple item changed observed will be returned in a single snapshot
    ///
    /// - Given:  The operation has started and the first query has completed.
    /// - When:
    ///    -  Observe multiple item changes.
    /// - Then:
    ///    - The items observed will be returned in the second snapshot
    ///
    func testMultipleItemChangesWillGenerateSecondSnapshot() throws {
        let firstSnapshot = expectation(description: "first query snapshot")
        let secondSnapshot = expectation(description: "second query snapshot")

        var querySnapshots = [DataStoreQuerySnapshot<Post>]()
        let operation = AWSDataStoreObseverQueryOperation(
            modelType: Post.self,
            modelSchema: Post.schema,
            predicate: nil,
            sortInput: nil,
            storageEngine: storageEngine,
            dataStorePublisher: dataStorePublisher,
            isDataStoreReady: false)

        let sink = operation.publisher.sink { completed in
            switch completed {
            case .finished:
                break
            case .failure(let error):
                XCTFail("Failed with error \(error)")
            }
        } receiveValue: { querySnapshot in
            querySnapshots.append(querySnapshot)
            if querySnapshots.count == 1 {
                firstSnapshot.fulfill()
            } else if querySnapshots.count == 2 {
                secondSnapshot.fulfill()
            }
        }
        let queue = OperationQueue()
        queue.addOperation(operation)
        wait(for: [firstSnapshot], timeout: 1)

        let post1 = try createPost(id: "1")
        let post2 = try createPost(id: "2")
        let post3 = try createPost(id: "3")
        dataStorePublisher.send(input: post1)
        dataStorePublisher.send(input: post2)
        dataStorePublisher.send(input: post3)
        wait(for: [secondSnapshot], timeout: 10)

        XCTAssertEqual(querySnapshots.count, 2)
        XCTAssertEqual(querySnapshots[0].itemsChanged.count, 0)
        XCTAssertEqual(querySnapshots[1].itemsChanged.count, 3)
        sink.cancel()
    }

    /// Multiple observed objects (more than the `.collect` count) in a short time window (shorter than the `.count`
    /// time) will return first snapshot with the count and the remaining in the second snapshot
    ///
    /// - Given:  The operation has started and the first query has completed.
    /// - When:
    ///    -  Observe 1100 item changes (beyond the `.collect` count of 1000)
    /// - Then:
    ///    - The items observed will perform a query and return 1000  items changed in the second query and the
    ///     remaining in the third query
    ///
    func testCollectOverMaxItemCountLimit() throws {
        let firstSnapshot = expectation(description: "first query snapshot")
        let secondSnapshot = expectation(description: "second query snapshot")
        let thirdSnapshot = expectation(description: "third query snapshot")

        var querySnapshots = [DataStoreQuerySnapshot<Post>]()
        let operation = AWSDataStoreObseverQueryOperation(
            modelType: Post.self,
            modelSchema: Post.schema,
            predicate: nil,
            sortInput: nil,
            storageEngine: storageEngine,
            dataStorePublisher: dataStorePublisher,
            isDataStoreReady: false)

        let sink = operation.publisher.sink { completed in
            switch completed {
            case .finished:
                break
            case .failure(let error):
                XCTFail("Failed with error \(error)")
            }
        } receiveValue: { querySnapshot in
            querySnapshots.append(querySnapshot)
            if querySnapshots.count == 1 {
                firstSnapshot.fulfill()
            } else if querySnapshots.count == 2 {
                secondSnapshot.fulfill()
            } else if querySnapshots.count == 3 {
                thirdSnapshot.fulfill()
            }
        }
        let queue = OperationQueue()
        queue.addOperation(operation)
        wait(for: [firstSnapshot], timeout: 1)

        for postId in 1 ... 1_100 {
            let post = try createPost(id: "\(postId)")
            dataStorePublisher.send(input: post)
        }

        wait(for: [secondSnapshot, thirdSnapshot], timeout: 10)

        XCTAssertEqual(querySnapshots.count, 3)
        XCTAssertEqual(querySnapshots[0].itemsChanged.count, 0)
        XCTAssertEqual(querySnapshots[1].itemsChanged.count, 1_000)
        XCTAssertEqual(querySnapshots[2].itemsChanged.count, 100)
        sink.cancel()
    }

    /// IsSynced is true when DataStore is ready
    ///
    /// - Given: DataStore is not ready, isSynced is false
    /// - When:
    ///    - DataStore ready event is emitted, item is observed
    /// - Then:
    ///    - A snapshot is generated with isSynced true
    ///
    func testIsSyncedWhenDataSoreIsReady() throws {
        let firstSnapshot = expectation(description: "first query snapshot")
        let secondSnapshot = expectation(description: "second query snapshot")
        var querySnapshots = [DataStoreQuerySnapshot<Post>]()
        let operation = AWSDataStoreObseverQueryOperation(
            modelType: Post.self,
            modelSchema: Post.schema,
            predicate: nil,
            sortInput: nil,
            storageEngine: storageEngine,
            dataStorePublisher: dataStorePublisher,
            isDataStoreReady: false)

        let sink = operation.publisher.sink { completed in
            switch completed {
            case .finished:
                break
            case .failure(let error):
                XCTFail("Failed with error \(error)")
            }
        } receiveValue: { querySnapshot in
            querySnapshots.append(querySnapshot)
            if querySnapshots.count == 1 {
                firstSnapshot.fulfill()
            } else if querySnapshots.count == 2 {
                secondSnapshot.fulfill()
            }
        }

        let queue = OperationQueue()
        queue.addOperation(operation)
        wait(for: [firstSnapshot], timeout: 1)

        let readyEventPayload = HubPayload(eventName: HubPayload.EventName.DataStore.ready)
        Amplify.Hub.dispatch(to: .dataStore, payload: readyEventPayload)

        let post1 = try createPost(id: "1")
        dataStorePublisher.send(input: post1)
        wait(for: [secondSnapshot], timeout: 10)

        XCTAssertEqual(querySnapshots.count, 2)
        XCTAssertFalse(querySnapshots[0].isSynced)
        XCTAssertTrue(querySnapshots[1].isSynced)
        sink.cancel()
    }

    /// Cancelling the subscription will no longer receive snapshots
    ///
    /// - Given:  subscriber to the operation
    /// - When:
    ///    - subscriber is cancelled
    /// - Then:
    ///    - no further snapshots are received
    ///
    func testSuccessfulSubscriptionCancel() throws {
        let firstSnapshot = expectation(description: "first query snapshot")
        let secondSnapshot = expectation(description: "second query snapshot")
        secondSnapshot.isInverted = true
        var querySnapshots = [DataStoreQuerySnapshot<Post>]()
        let operation = AWSDataStoreObseverQueryOperation(
            modelType: Post.self,
            modelSchema: Post.schema,
            predicate: nil,
            sortInput: nil,
            storageEngine: storageEngine,
            dataStorePublisher: dataStorePublisher,
            isDataStoreReady: false)

        let sink = operation.publisher.sink { completed in
            switch completed {
            case .finished:
                break
            case .failure(let error):
                XCTFail("Failed with error \(error)")
            }
        } receiveValue: { snapshot in
            querySnapshots.append(snapshot)
            if querySnapshots.count == 1 {
                firstSnapshot.fulfill()
            } else if querySnapshots.count == 2 {
                secondSnapshot.fulfill()
                XCTFail("Should not receive second snapshot after cancelling")
            }
        }

        let queue = OperationQueue()
        queue.addOperation(operation)
        wait(for: [firstSnapshot], timeout: 1)
        sink.cancel()
        let post1 = try createPost(id: "1")
        dataStorePublisher.send(input: post1)

        wait(for: [secondSnapshot], timeout: 1)
        XCTAssertTrue(operation.isCancelled)
        XCTAssertTrue(operation.isFinished)
    }

    /// Cancelling the underlying operation will emit a completion to the subscribers
    ///
    /// - Given:  subscriber to the operation
    /// - When:
    ///    - operation is cancelled
    /// - Then:
    ///    - the subscriber receives a cancellation
    ///
    func testSuccessfulOperationCancel() throws {
        let firstSnapshot = expectation(description: "first query snapshot")
        let secondSnapshot = expectation(description: "second query snapshot")
        secondSnapshot.isInverted = true
        let completedEvent = expectation(description: "should have completed")
        var querySnapshots = [DataStoreQuerySnapshot<Post>]()
        let operation = AWSDataStoreObseverQueryOperation(
            modelType: Post.self,
            modelSchema: Post.schema,
            predicate: nil,
            sortInput: nil,
            storageEngine: storageEngine,
            dataStorePublisher: dataStorePublisher,
            isDataStoreReady: false)

        let sink = operation.publisher.sink { completed in
            switch completed {
            case .finished:
                completedEvent.fulfill()
            case .failure(let error):
                XCTFail("Failed with error \(error)")
            }
        } receiveValue: { snapshot in
            querySnapshots.append(snapshot)
            if querySnapshots.count == 1 {
                firstSnapshot.fulfill()
            } else if querySnapshots.count == 2 {
                secondSnapshot.fulfill()
                XCTFail("Should not receive second snapshot after cancelling")
            }
        }

        let queue = OperationQueue()
        queue.addOperation(operation)
        wait(for: [firstSnapshot], timeout: 1)
        operation.cancel()
        let post1 = try createPost(id: "1")
        dataStorePublisher.send(input: post1)

        wait(for: [secondSnapshot], timeout: 1)
        wait(for: [completedEvent], timeout: 1)
        XCTAssertTrue(operation.isCancelled)
        XCTAssertTrue(operation.isFinished)
    }

    // MARK: - Helpers

    func createPost(id: String) throws -> MutationEvent {
        try MutationEvent(model: Post(id: id,
                                      title: "model1",
                                      content: "content1",
                                      createdAt: .now()),
                          modelSchema: Post.schema,
                          mutationType: MutationEvent.MutationType.create)
    }

}
