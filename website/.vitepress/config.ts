import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Converge',
  description: 'One model for all background work',
  // Required while the site is served from a project page with no custom
  // domain (https://garearc.github.io/converge/). Remove this if a custom
  // domain is ever configured to serve the site from the root.
  base: '/converge/',
  srcDir: './.generated',
  outDir: './.dist',
  cleanUrls: true,
  themeConfig: {
    nav: [
      { text: 'Glossary', link: '/glossary' },
    ],
    search: { provider: 'local' },
    socialLinks: [{ icon: 'github', link: 'https://github.com/GareArc/converge' }],
    sidebar: [
      {
        text: 'Documentation',
        items: [
          { text: 'Overview', link: '/' },
          { text: 'Glossary', link: '/glossary' },
        ],
      },
    ],
  },
})
