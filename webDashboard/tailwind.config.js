/** @type {import("tailwindcss").Config} */
export default {
    content: ["./index.html", "./src/**/*.{js,jsx}"],
    darkMode: "class",
    theme: {
        extend: {
            colors: {
                // Command-centre palette: dark-mode first so screens stay readable in low-light field posts
                canvas: "#08120f",
                panel: "#0e1c17",
                panelRaised: "#12231c",
                hairline: "#234336",
                ink: "#e8f2ed",
                muted: "#9bb1a6",
                accent: "#57d89b",
                accentCool: "#5aa8ff",
                caution: "#f3bb59",
                danger: "#ef6b72",
            },
            fontFamily: {
                sans: ["Inter", "Segoe UI", "system-ui", "sans-serif"],
                mono: ["JetBrains Mono", "Consolas", "monospace"],
            },
            boxShadow: {
                panel: "0 18px 60px rgba(0, 0, 0, 0.26)",
            },
            keyframes: {
                slideIn: {
                    "0%": { transform: "translateX(24px)", opacity: "0" },
                    "100%": { transform: "translateX(0)", opacity: "1" },
                },
            },
            animation: {
                slideIn: "slideIn 220ms ease-out",
            },
        },
    },
    plugins: [],
};
