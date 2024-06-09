import { defineConfig } from 'vite';
import commonjs from 'vite-plugin-commonjs'; // Import the commonjs plugin

export default defineConfig({
    plugins: [commonjs()], // Use the commonjs plugin
});
