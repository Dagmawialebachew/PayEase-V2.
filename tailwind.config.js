/** @type {import('tailwindcss').Config} */
module.exports = {
content: [
  "./frontend/**/*.html",      // Look inside the frontend folder for HTML
  "./frontend/**/*.js",        // Look inside the frontend folder for JS
  "./*.html",                  // Keep this just in case
  "./js/**/*.js"               // Keep this just in case
],
  theme: {
    extend: {
      colors: {
        premium: {
          dark: "#0a0a0c", // Deep OLED Black
          glass: "rgba(255, 255, 255, 0.05)",
          neonBlue: "#00d2ff",
          neonPurple: "#9d50bb",
          neonGreen: "#00ff87",
          neonRed: "#ff0055",
        }

      },
      backgroundImage: {
        'cinematic-gradient': "linear-gradient(135deg, #667eea 0%, #764ba2 100%)",
        'neon-purity': "linear-gradient(90deg, #00d2ff 0%, #9d50bb 100%)",
      },
      backdropBlur: {
        xs: '2px',
      },
      borderRadius: {
        'iphone': '2.5rem',
      },
      animation: {
        'glow': 'glow 3s infinite alternate',
      },
      keyframes: {
        glow: {
          '0%': { boxShadow: '0 0 5px rgba(0, 210, 255, 0.2)' },
          '100%': { boxShadow: '0 0 20px rgba(0, 210, 255, 0.6)' },
        }
      }
    },
  },
  plugins: [],
}