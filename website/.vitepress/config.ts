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
      { text: 'Guide', link: '/guide/' },
      { text: 'Cookbook', link: '/cookbook/' },
      { text: 'Reference', link: '/reference/kernel' },
      { text: 'Glossary', link: '/glossary' },
    ],
    search: { provider: 'local' },
    socialLinks: [{ icon: 'github', link: 'https://github.com/GareArc/converge' }],
    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Overview', link: '/guide/' },
          { text: '1. Your first job', link: '/guide/01-first-job' },
          { text: '2. One job, many things', link: '/guide/02-ids' },
          { text: '3. Telling a job to look sooner', link: '/guide/03-notifications' },
          { text: '4. When the message is the work', link: '/guide/04-worker' },
          { text: '5. Where a job runs', link: '/guide/05-run-modes' },
          { text: '6. Taking it to production', link: '/guide/06-production' },
          { text: '7. Testing a job', link: '/guide/07-testing' },
        ],
      },
      {
        text: 'Cookbook',
        items: [
          { text: 'Overview', link: '/cookbook/' },
          { text: 'Work that takes a while', link: '/cookbook/durable-work' },
          { text: 'Waiting for something to become true', link: '/cookbook/event-driven' },
          { text: 'A queue somebody else owns', link: '/cookbook/foreign-queue' },
          { text: 'Jobs that end', link: '/cookbook/lifecycle' },
          { text: 'Outbox and inbox', link: '/cookbook/outbox-inbox' },
          { text: 'The safety net', link: '/cookbook/safety-net' },
          { text: 'Credential sync from a Python service', link: '/cookbook/python-producer' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'Kernel', link: '/reference/kernel' },
          { text: 'Reconcile', link: '/reference/reconcile' },
          { text: 'Worker', link: '/reference/worker' },
          { text: 'Operations', link: '/reference/operations' },
          { text: 'Wire', link: '/reference/wire' },
          { text: 'Adapters and test support', link: '/reference/adapters' },
          { text: 'Converge terms in other systems', link: '/reference/prior-art' },
        ],
      },
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
