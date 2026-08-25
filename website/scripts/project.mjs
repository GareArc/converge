import { cp, rm } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const here = dirname(fileURLToPath(import.meta.url))
const site = join(here, '..')
const source = join(site, '..', 'docs')
const target = join(site, '.generated')

await rm(target, { recursive: true, force: true })
await cp(source, target, {
  recursive: true,
  filter: (path) => !path.includes('/superpowers'),
})
console.log(`projected ${source} -> ${target}`)
