import type { Config } from 'tailwindcss';

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
        display: ['Outfit', 'Inter', 'system-ui', 'sans-serif'],
      },
      colors: {
        // Warm charcoal override of Tailwind's cool slate — retunes every
        // existing bg-slate-*/text-slate-*/border-slate-* usage app-wide.
        slate: {
          50: '#FAF7F2',
          100: '#F1EBE2',
          200: '#E5DACB',
          300: '#C9B9A4',
          400: '#9A8E7D',
          500: '#716657',
          600: '#50483C',
          700: '#3A332A',
          800: '#27211A',
          900: '#17130E',
          950: '#0F0C08',
        },
        // Warm amber/copper accent system ("ember")
        ember: {
          50: '#FEF7EC',
          100: '#FDEBD3',
          200: '#FAD5A5',
          300: '#F6BA71',
          400: '#F2A65A',
          500: '#E88F3C',
          600: '#D0742A',
          700: '#AD5A22',
          800: '#8C4820',
          900: '#723C1E',
        },
        brand: {
          50: '#eff6ff',
          100: '#dbeafe',
          200: '#bfdbfe',
          300: '#93c5fd',
          400: '#60a5fa',
          500: '#3b82f6',
          600: '#2563eb',
          700: '#1d4ed8',
          800: '#1e40af',
          900: '#1e3a8a',
        },
      },
    },
  },
  plugins: [],
};
export default config;
