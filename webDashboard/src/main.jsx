/**
 * Browser entry point.
 *
 * HashRouter is deliberate: the Pi serves the build as static files behind FastAPI, so a deep link
 * such as /species/1 would otherwise be resolved as an API path before React ever mounts.
 */

import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { HashRouter } from "react-router-dom";
import App from "./App.jsx";
import "./index.css";

createRoot(document.getElementById("root")).render(
    <StrictMode>
        <HashRouter>
            <App />
        </HashRouter>
    </StrictMode>,
);
