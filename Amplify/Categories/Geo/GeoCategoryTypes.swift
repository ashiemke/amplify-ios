//
// Copyright Amazon.com Inc. or its affiliates.
// All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

import Foundation
import CoreLocation

/// Results handler for Amplify Geo.
public typealias GeoResultsHandler<T> = (Result<T, Error>) -> Void

/// A pair of coordinates to represent a location (point).
public struct Coordinates {
    /// The latitude of the location.
    let latitude: Double
    /// The longitude of the location.
    let longitude: Double

    /// Initializer
    public init(latitude: Double, longitude: Double) {
        self.latitude = latitude
        self.longitude = longitude
    }
}

public extension Coordinates {
    /// Initialize a Location from a CLLocationCoordinate2D
    /// - Parameter location: The CLLocationCoordinate2D to use to initialize the
    /// Location.
    init(_ coordinates: CLLocationCoordinate2D) {
        self.init(latitude: coordinates.latitude, longitude: coordinates.longitude)
    }
}

public extension CLLocationCoordinate2D {
    /// Initialize a Location from a CLLocationCoordinate2D
    /// - Parameter location: The CLLocationCoordinate2D to use to initialize the
    /// Location.
    init(_ coordinates: Coordinates) {
        self.init(latitude: coordinates.latitude, longitude: coordinates.longitude)
    }
}

/// A bounding box defined by southwest and northeast corners.
public struct BoundingBox {
    /// The southwest corner of the bounding box.
    let southwest: Coordinates
    /// The northeast corner of the bounding box.
    let northeast: Coordinates

    /// Initializer
    public init(southwest: Coordinates, northeast: Coordinates) {
        self.southwest = southwest
        self.northeast = northeast
    }
}

/// The area to search.
public enum SearchArea {
    /// Searches for results closest to the given coordinates.
    case near(Coordinates)
    /// Filters the results by returning only Places within the provided bounding box.
    case within(BoundingBox)
}

public extension SearchArea {
    /// Creates a SearchArea that returns results closest to the given
    /// CLLocationCoordinate2D.
    /// - Parameter coordinates: The coordinates for the search area.
    /// - Returns: The SearchArea.
    static func near(_ coordinates: CLLocationCoordinate2D) -> SearchArea {
        .near(Coordinates(coordinates))
    }
}

/// A place defined by a label, location (point), and optional additional locality
/// information.
public struct Place {
    /// The label of the place.
    let label: String
    /// The coordinates of the place.
    let coordinates: Coordinates
    /// The country of the place.
    let country: String?
    /// The region of the place.
    let region: String?
    /// The subRegion of the place.
    let subRegion: String?
    /// The municipality of the place.
    let municipality: String?

    /// Initializer
    public init(label: String, coordinates: Coordinates, country: String?, region: String?, subRegion: String?, municipality: String?) {
        self.label = label
        self.coordinates = coordinates
        self.country = country
        self.region = region
        self.subRegion = subRegion
        self.municipality = municipality
    }
}

/// Identifies the name and style for a map resource.
public struct MapStyle {
    /// The name of the map resource.
    let mapName: String
    /// The map style selected from an available provider.
    let style: String
    /// The URL to retrieve the style descriptor of the map resource.
    let styleURL: URL

    /// Initializer
    public init(mapName: String, style: String, styleURL: URL) {
        self.mapName = mapName
        self.style = style
        self.styleURL = styleURL
    }
}
