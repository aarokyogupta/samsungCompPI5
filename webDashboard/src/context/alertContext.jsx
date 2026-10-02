/**
 * Global alert context.
 *
 * Mounted once at the root so the WebSocket survives route changes: an operator can be reading a
 * risk report when a gunshot classification lands and still get the toast overlay.
 */

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { AlertSocket, PRIORITY_ORDER } from "../services/api.js";

// Configuration
const LIVE_FEED_LIMIT = 120;
const TOAST_LIMIT = 4;
// Anything at or above this tier interrupts the operator with a toast; quieter events only fill the feed
const TOAST_PRIORITY_FLOOR = "ELEVATED";
const TOAST_DISMISS_MS = 12000;

const AlertContext = createContext(null);

export const priorityRank = (priority) => {
    const index = PRIORITY_ORDER.indexOf(String(priority || "").toUpperCase());
    return index === -1 ? 0 : index;
};

export function AlertProvider({ children }) {
    const [connectionState, setConnectionState] = useState("idle");
    const [alerts, setAlerts] = useState([]);
    const [toasts, setToasts] = useState([]);
    const [welcome, setWelcome] = useState(null);
    const socketRef = useRef(null);

    const dismissToast = useCallback((eventID) => {
        setToasts((current) => current.filter((toast) => toast.event_id !== eventID));
    }, []);

    useEffect(() => {
        const socket = new AlertSocket({ clientType: "web", minPriority: "INFO" });
        socketRef.current = socket;

        const releaseState = socket.onStateChange(setConnectionState);
        const releaseFrames = socket.onFrame((frame) => {
            if (frame.type === "welcome") {
                setWelcome(frame);
                return;
            }

            if (frame.type !== "alert") {
                return;
            }

            // Catch-up frames replayed on reconnect are kept in the feed but must not re-ring the alarm
            setAlerts((current) => [frame, ...current].slice(0, LIVE_FEED_LIMIT));

            if (!frame.replayed && priorityRank(frame.priority) >= priorityRank(TOAST_PRIORITY_FLOOR)) {
                setToasts((current) => [frame, ...current].slice(0, TOAST_LIMIT));
            }
        });

        socket.connect();

        return () => {
            releaseState();
            releaseFrames();
            socket.close();
            socketRef.current = null;
        };
    }, []);

    // Toasts expire on their own so an unattended screen does not silt up with stale banners
    useEffect(() => {
        if (toasts.length === 0) {
            return undefined;
        }

        const timer = window.setTimeout(() => {
            setToasts((current) => current.slice(0, -1));
        }, TOAST_DISMISS_MS);

        return () => window.clearTimeout(timer);
    }, [toasts]);

    const value = useMemo(
        () => ({
            connectionState,
            alerts,
            toasts,
            welcome,
            dismissToast,
            clearAlerts: () => setAlerts([]),
            subscribeZones: (zones, minPriority) => socketRef.current?.subscribe({ zones, minPriority }),
            unsubscribeZones: () => socketRef.current?.unsubscribe(),
        }),
        [connectionState, alerts, toasts, welcome, dismissToast],
    );

    return <AlertContext.Provider value={value}>{children}</AlertContext.Provider>;
}

export function useAlerts() {
    const context = useContext(AlertContext);
    if (!context) {
        throw new Error("useAlerts must be used inside an AlertProvider.");
    }
    return context;
}
