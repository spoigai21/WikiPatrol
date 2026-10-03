// Turn a MediaWiki table diff (action=compare, prop=diff) into compact plain text for a model.
//
// Lines are prefixed "- " (removed) and "+ " (added). Inside a changed line only the changed words
// are marked: [-removed-] and {+added+}, with a little context, so a one-word change in a long
// paragraph costs a few dozen tokens instead of the whole paragraph twice.

const CONTEXT = 100;
const MAX_SPANS_PER_LINE = 6;

const decode = (s: string) =>
  s
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#0?39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&');
const stripTags = (s: string) => decode(s.replace(/<[^>]+>/g, ''));

const OPEN = '\u0001';
const CLOSE = '\u0002';

function renderCell(cellHtml: string, sign: '-' | '+'): string {
  const marked = cellHtml.replace(/<(ins|del)[^>]*class="[^"]*diffchange[^"]*"[^>]*>([\s\S]*?)<\/\1>/g, `${OPEN}$2${CLOSE}`);
  const text = stripTags(marked.replace(/<\/?div[^>]*>/g, '')).trim();
  if (!text.replace(/[\u0001\u0002]/g, '').trim()) return '';
  if (!text.includes(OPEN)) return `${sign} ${text}`;
  const [l, r] = sign === '-' ? ['[-', '-]'] : ['{+', '+}'];
  const parts: string[] = [];
  const re = /\u0001([\s\S]*?)\u0002/g;
  let m: RegExpExecArray | null;
  let last = 0;
  while ((m = re.exec(text))) {
    if (parts.length === MAX_SPANS_PER_LINE) {
      parts.push('…(more changes in this line)');
      break;
    }
    const clean = (s: string) => s.replace(/[\u0001\u0002]/g, '');
    const before = clean(text.slice(Math.max(last, m.index - CONTEXT), m.index));
    const after = clean(text.slice(re.lastIndex, re.lastIndex + CONTEXT));
    parts.push(`…${before}${l}${m[1]}${r}${after}…`);
    last = re.lastIndex;
  }
  return `${sign} ${parts.join(' ')}`;
}

/** Plain-text rendering of a compare-API diff body, capped at `maxChars`. */
export function renderDiff(html: string, maxChars = 3000): { text: string; truncated: boolean } {
  const lines: string[] = [];
  for (const m of html.matchAll(/<td class="diff-(deleted|added)line[^"]*"[^>]*>([\s\S]*?)<\/td>/g)) {
    const line = renderCell(m[2]!, m[1] === 'deleted' ? '-' : '+');
    if (line) lines.push(line);
  }
  const text = lines.join('\n');
  return text.length > maxChars ? { text: text.slice(0, maxChars) + '\n[diff truncated]', truncated: true } : { text, truncated: false };
}
