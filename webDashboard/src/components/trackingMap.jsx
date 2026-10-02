/**
 * Geospatial tracking map.
 *
 * Two layers are drawn: the clustered heat nodes returned as GeoJSON by GET /analytics/spatial, and
 * the raw recent fixes from GET /telemetry/data drawn as a movement vector polyline.
 */

import { useEffect, useMemo, useRef, useState } from "react";
import { CircleMarker, MapContainer, Polyline, Popup, TileLayer, useMap, useMapEvents } from "react-leaflet";

// Configuration
const DEFAULT_CENTRE = [-2.334, 34.821];
const DEFAULT_ZOOM = 9;
// Plain OSM tiles need no API key, which matters on an isolated field network; the dark look is
// applied as a CSS filter in index.css rather than by depending on a keyed dark basemap provider
const TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const TILE_ATTRIBUTION = "&copy; OpenStreetMap contributors";
// Sub-second viewport events would hammer the Pi, so bounding-box reloads are debounced
const VIEWPORT_DEBOUNCE_MS = 450;

const clampLatitude = (value) => Math.min(90, Math.max(-90, value));
const clampLongitude = (value) => Math.min(180, Math.max(-180, value));

const clusterRadius = (pointCount, maxCount) => {
    if (!maxCount) {
        return 8;
    }
    return 8 + Math.sqrt(pointCount / maxCount) * 18;
};

/** Reports the map's bounding box upward so the parent can cull the query to what is on screen. */
function ViewportReporter({ onViewportChange }) {
    const timerRef = useRef(null);
    const map = useMap();

    const publish = () => {
        if (timerRef.current) {
            window.clearTimeout(timerRef.current);
        }
        timerRef.current = window.setTimeout(() => {
            const bounds = map.getBounds();
                // Leaflet happily reports latitudes past the poles and longitudes past the date line once the
                // world wraps, which the API rejects with a 422, so the viewport is clamped before it is sent
                const north = clampLatitude(bounds.getNorth());
                const south = clampLatitude(bounds.getSouth());
                const east = clampLongitude(bounds.getEast());
                const west = clampLongitude(bounds.getWest());
                if (south >= north || east === west) {
                    return;
                }
                onViewportChange({ north, south, east, west });
            }, VIEWPORT_DEBOUNCE_MS);
        };

    useMapEvents({ moveend: publish, zoomend: publish });

    useEffect(() => {
        publish();
        return () => {
            if (timerRef.current) {
                window.clearTimeout(timerRef.current);
            }
        };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    return null;
}

/** Recentres the map when a new track arrives, without fighting a manual pan already in progress. */
function TrackFollower({ position, enabled }) {
    const map = useMap();

    useEffect(() => {
        if (enabled && position) {
            map.panTo(position, { animate: true, duration: 0.6 });
        }
    }, [enabled, position, map]);

    return null;
}

export function TrackingMap({ clusters = [], track = [], onViewportChange, follow = false, height = 440 }) {
    const [activeCentre] = useState(DEFAULT_CENTRE);

    const maxPointCount = useMemo(
        () => clusters.reduce((peak, feature) => Math.max(peak, feature.properties?.point_count || 0), 0),
        [clusters],
    );

    // GeoJSON is [longitude, latitude]; Leaflet is [latitude, longitude], so every point is flipped once here
    const trackLine = useMemo(
        () => track.filter((fix) => fix.latitude != null && fix.longitude != null).map((fix) => [fix.latitude, fix.longitude]),
        [track],
    );

    return (
        <div className="overflow-hidden rounded-2xl border border-hairline" style={{ height }}>
            <MapContainer center={activeCentre} zoom={DEFAULT_ZOOM} className="h-full w-full" scrollWheelZoom>
                <TileLayer url={TILE_URL} attribution={TILE_ATTRIBUTION} />
                {onViewportChange && <ViewportReporter onViewportChange={onViewportChange} />}
                <TrackFollower position={trackLine[0]} enabled={follow} />

                {clusters.map((feature) => {
                    const [longitude, latitude] = feature.geometry.coordinates;
                    const properties = feature.properties || {};
                    return (
                        <CircleMarker
                            key={`cluster-${properties.cluster_id}`}
                            center={[latitude, longitude]}
                            radius={clusterRadius(properties.point_count, maxPointCount)}
                            pathOptions={{ color: "#57d89b", fillColor: "#57d89b", fillOpacity: 0.28, weight: 1 }}
                        >
                            <Popup>
                                <div className="text-xs">
                                    <p className="font-semibold">Cluster {properties.cluster_id}</p>
                                    <p>{properties.point_count} points</p>
                                    {properties.animal_count != null && <p>{properties.animal_count} animals</p>}
                                </div>
                            </Popup>
                        </CircleMarker>
                    );
                })}

                {trackLine.length > 1 && (
                    <Polyline positions={trackLine} pathOptions={{ color: "#5aa8ff", weight: 2, opacity: 0.85 }} />
                )}

                {trackLine.length > 0 && (
                    <CircleMarker
                        center={trackLine[0]}
                        radius={7}
                        pathOptions={{ color: "#f3bb59", fillColor: "#f3bb59", fillOpacity: 0.9, weight: 2 }}
                    >
                        <Popup>
                            <div className="text-xs">
                                <p className="font-semibold">Latest fix</p>
                                <p>{track[0]?.recorded_at}</p>
                                {track[0]?.speed_kmh != null && <p>{track[0].speed_kmh} km/h</p>}
                            </div>
                        </Popup>
                    </CircleMarker>
                )}
            </MapContainer>
        </div>
    );
}

export default TrackingMap;
