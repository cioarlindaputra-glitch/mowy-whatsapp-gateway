import fs from 'node:fs/promises'

const file = 'node_modules/@whiskeysockets/baileys/lib/Socket/messages-recv.js'
let source

try {
  source = await fs.readFile(file, 'utf8')
} catch {
  console.error('Baileys output file was not found; expected package layout may have changed.')
  process.exit(1)
}

const unsafe = 'const count = +countChild.attrs.value;'
const guard = "if (!countChild?.attrs?.value) { logger.warn({ node }, 'ignored encrypt notification without pre-key count'); return }\n\t\t\tconst count = +countChild.attrs.value;"
const occurrences = source.split(unsafe).length - 1

if (occurrences === 0 && source.includes('ignored encrypt notification without pre-key count')) {
  console.log('Baileys encrypt notification guard already installed.')
  process.exit(0)
}

if (occurrences !== 1) {
  throw new Error(`Expected one unsafe pre-key count access, found ${occurrences}; no patch applied.`)
}

await fs.writeFile(file, source.replace(unsafe, guard))
console.log('Patched Baileys encrypt notification count guard.')
