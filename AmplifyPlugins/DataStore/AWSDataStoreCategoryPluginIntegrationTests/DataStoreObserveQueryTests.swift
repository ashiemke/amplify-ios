//
// Copyright Amazon.com Inc. or its affiliates.
// All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

import XCTest

import AmplifyPlugins
import AWSPluginsCore

@testable import Amplify
@testable import AmplifyTestCommon
@testable import AWSDataStoreCategoryPlugin

@available(iOS 13.0, *)
class DataStoreObserveQueryTests: SyncEngineIntegrationTestBase {

    /// ObserveQuery API will eventually return query snapshot with `isSynced` true
    ///
    /// - Given: DataStore is cleared
    /// - When:
    ///    - ObserveQuery API is called to start the sync engine
    /// - Then:
    ///    - Eventually one of the query snapshots will be returned with `isSynced` true
    ///
    func testObserveQueryWithIsSyncedTrueEvent() throws {
        let started = expectation(description: "Amplify started")
        try startAmplify {
            started.fulfill()
        }
        wait(for: [started], timeout: 2)
        _ = Amplify.DataStore.clear()
        let snapshotWithIsSynced = expectation(description: "query snapshot with ready event")
        let sink = Amplify.DataStore.observeQuery(for: Post.self).sink { completed in
            switch completed {
            case .finished:
                break
            case .failure(let error):
                XCTFail("\(error)")
            }
        } receiveValue: { querySnapshot in
            if querySnapshot.isSynced {
                snapshotWithIsSynced.fulfill()
            }
        }

        _ = Amplify.DataStore.save(Post(title: "title", content: "content", createdAt: .now()))
        wait(for: [snapshotWithIsSynced], timeout: 100)
        sink.cancel()
    }

    /// A query snapshot with the recently saved post should be the last item when
    /// sort order is provided as ascending createdAt field
    func testObserveQueryWithSort() throws {
        let started = expectation(description: "Amplify started")
        try startAmplify {
            started.fulfill()
        }
        wait(for: [started], timeout: 2)
        _ = Amplify.DataStore.clear()

        let post = Post(title: "title", content: "content", createdAt: .now())
        let snapshotWithSavedPost = expectation(description: "query snapshot with saved post")
        let sink = Amplify.DataStore.observeQuery(for: Post.self,
                                                  sort: .ascending(Post.keys.createdAt))
            .sink { completed in
                switch completed {
                case .finished:
                    break
                case .failure(let error):
                    XCTFail("\(error)")
                }
            } receiveValue: { querySnapshot in
                if querySnapshot.itemsChanged.contains(where: { mutationEvent in
                    mutationEvent.modelId == post.id
                }) {

                    if let lastPost = querySnapshot.items.last {
                        XCTAssertEqual(lastPost.id, post.id)
                        snapshotWithSavedPost.fulfill()
                    }
                }
            }
        _ = Amplify.DataStore.save(post)
        wait(for: [snapshotWithSavedPost], timeout: 100)
        sink.cancel()
    }
}
