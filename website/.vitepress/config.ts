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
      { text: 'Guide', link: '/guide/01-first-job' },
      { text: 'Cookbook', link: '/cookbook/scenario-a-safety-net' },
      { text: 'Reference', link: '/reference/kernel' },
      { text: 'Glossary', link: '/glossary' },
    ],
    search: { provider: 'local' },
    socialLinks: [{ icon: 'github', link: 'https://github.com/GareArc/converge' }],
    sidebar: {
      '/guide/': [
        {
          text: 'The path',
          items: [
            { text: '1. A first job', link: '/guide/01-first-job' },
            { text: '2. Many things to check', link: '/guide/02-ids' },
            { text: '3. Reacting to events', link: '/guide/03-triggers' },
            { text: '4. The other kind of job', link: '/guide/04-worker' },
            { text: '5. More than one copy', link: '/guide/05-run-modes' },
            { text: '6. Going to production', link: '/guide/06-production' },
          ],
        },
        {
          text: 'When you need it',
          items: [
            { text: '7. Stale writes', link: '/guide/07-versions' },
            { text: '8. Testing your jobs', link: '/guide/08-testing' },
            { text: '9. Running it in production', link: '/guide/09-operations' },
            { text: '10. Seeing what it is doing', link: '/guide/10-observability' },
          ],
        },
      ],
    },
  },
})
