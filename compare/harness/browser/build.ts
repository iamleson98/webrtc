// Bundles harness.ts with the LMS web app's real call client (bun build.ts <lms-web dir> <out.js>).
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';

const [lmsWeb, out] = process.argv.slice(2);
if (!lmsWeb || !out) throw new Error('usage: bun build.ts <lms-web dir> <out.js>');
const here = import.meta.dir;
const client = resolve(lmsWeb, 'src/lib/features/calls/call-client.ts');
const entry = resolve(here, '.entry.ts');
writeFileSync(entry, (await Bun.file(resolve(here, 'harness.ts')).text()).replace('LMS_CALL_CLIENT', client));

const result = await Bun.build({
	entrypoints: [entry],
	target: 'browser',
	plugins: [
		{
			name: 'realtime-stub',
			setup(build) {
				build.onResolve({ filter: /^\$lib\/realtime\/socket\.svelte$/ }, () => ({
					path: resolve(here, 'realtime-stub.ts'),
				}));
			},
		},
	],
});
if (!result.success) {
	console.error(result.logs.join('\n'));
	process.exit(1);
}
mkdirSync(dirname(resolve(out)), { recursive: true });
writeFileSync(out, await result.outputs[0].text());
